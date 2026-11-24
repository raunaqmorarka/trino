/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution.executor.timesharing;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.airlift.stats.CounterStat;
import io.trino.execution.TaskManagerConfig;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.math.DoubleMath.roundToLong;
import static java.math.RoundingMode.HALF_UP;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class MultilevelSplitQueue
{
    static final int[] LEVEL_THRESHOLD_SECONDS = {0, 1, 10, 60, 300};
    static final long LEVEL_CONTRIBUTION_CAP = SECONDS.toNanos(30);

    private final ConcurrentSkipListSet<PrioritizedSplitRunner>[] levelWaitingSplits;

    // one permit per waiting split, so a taker blocks only when no split can be waiting
    private final Semaphore waitingSplitPermits = new Semaphore(0);
    // permits that takers hold for splits removed from the queue
    private final AtomicInteger orphanedPermits = new AtomicInteger();
    private final AtomicInteger waitingSplitCount = new AtomicInteger();

    private final AtomicLong[] levelScheduledTime;

    private final AtomicLong[] levelMinPriority;
    private final CounterStat[] selectedLevelCounters;

    private final double levelTimeMultiplier;

    @Inject
    public MultilevelSplitQueue(TaskManagerConfig taskManagerConfig)
    {
        this(taskManagerConfig.getLevelTimeMultiplier().doubleValue());
    }

    public MultilevelSplitQueue(double levelTimeMultiplier)
    {
        this.levelScheduledTime = new AtomicLong[LEVEL_THRESHOLD_SECONDS.length];
        this.levelMinPriority = new AtomicLong[LEVEL_THRESHOLD_SECONDS.length];
        this.levelWaitingSplits = new ConcurrentSkipListSet[LEVEL_THRESHOLD_SECONDS.length];
        this.selectedLevelCounters = new CounterStat[LEVEL_THRESHOLD_SECONDS.length];

        for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.length; level++) {
            levelScheduledTime[level] = new AtomicLong();
            levelMinPriority[level] = new AtomicLong(-1);
            levelWaitingSplits[level] = new ConcurrentSkipListSet<>();
            selectedLevelCounters[level] = new CounterStat();
        }

        this.levelTimeMultiplier = levelTimeMultiplier;
    }

    private void addLevelTime(int level, long nanos)
    {
        levelScheduledTime[level].addAndGet(nanos);
    }

    /**
     * During periods of time when a level has no waiting splits, it will not accumulate
     * scheduled time and will fall behind relative to other levels.
     * <p>
     * This can cause temporary starvation for other levels when splits do reach the
     * previously-empty level.
     * <p>
     * To prevent this we set the scheduled time for levels which were empty to the expected
     * scheduled time.
     */
    public void offer(PrioritizedSplitRunner split)
    {
        checkArgument(split != null, "split is null");

        split.setReady();
        // TimeSharingTaskExecutor destroys the task handle before it removes the task's splits from this queue
        if (split.getTaskHandle().isDestroyed()) {
            return;
        }
        waitingSplitCount.incrementAndGet();
        addWaitingSplit(split);
        waitingSplitPermits.release();
    }

    public PrioritizedSplitRunner take()
            throws InterruptedException
    {
        waitingSplitPermits.acquire();
        while (true) {
            PrioritizedSplitRunner result = pollSplit();
            if (result == null) {
                if (tryTakeOrphanedPermit()) {
                    waitingSplitPermits.acquire();
                }
                else {
                    // a split for this permit is waiting in a level this taker already scanned
                    Thread.onSpinWait();
                }
                continue;
            }
            // a split whose level changed goes back to its new level, and the permit stays with this taker
            if (result.updateLevelPriority()) {
                // a split of a destroyed task is dropped together with its permit
                if (result.getTaskHandle().isDestroyed()) {
                    waitingSplitCount.decrementAndGet();
                    waitingSplitPermits.acquire();
                    continue;
                }
                result.setReady();
                addWaitingSplit(result);
                continue;
            }
            int selectedLevel = result.getPriority().getLevel();
            levelMinPriority[selectedLevel].set(result.getPriority().getLevelPriority());
            waitingSplitCount.decrementAndGet();
            selectedLevelCounters[selectedLevel].update(1);
            return result;
        }
    }

    private boolean tryTakeOrphanedPermit()
    {
        int orphaned = orphanedPermits.get();
        while (orphaned > 0) {
            if (orphanedPermits.compareAndSet(orphaned, orphaned - 1)) {
                return true;
            }
            orphaned = orphanedPermits.get();
        }
        return false;
    }

    private void addWaitingSplit(PrioritizedSplitRunner split)
    {
        int level = split.getPriority().getLevel();
        if (levelWaitingSplits[level].isEmpty()) {
            // Accesses to levelScheduledTime are not synchronized, so we have a data race
            // here - our level time math will be off. However, the staleness is bounded by
            // the fact that only running splits that complete during this computation
            // can update the level time. Therefore, this is benign.
            long level0Time = getLevel0TargetTime();
            long levelExpectedTime = (long) (level0Time / Math.pow(levelTimeMultiplier, level));
            // concurrent offers to an empty level all set the same expected time
            levelScheduledTime[level].set(levelExpectedTime);
        }

        levelWaitingSplits[level].add(split);
    }

    /**
     * Trino attempts to give each level a target amount of scheduled time, which is configurable
     * using levelTimeMultiplier.
     * <p>
     * This function selects the level that has the lowest ratio of actual to the target time
     * with the objective of minimizing deviation from the target scheduled time. From this level,
     * we pick the split with the lowest priority.
     */
    private PrioritizedSplitRunner pollSplit()
    {
        while (true) {
            long targetScheduledTime = getLevel0TargetTime();
            double worstRatio = 1;
            int selectedLevel = -1;
            for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.length; level++) {
                if (!levelWaitingSplits[level].isEmpty()) {
                    long levelTime = levelScheduledTime[level].get();
                    double ratio = levelTime == 0 ? 0 : targetScheduledTime / (1.0 * levelTime);
                    if (selectedLevel == -1 || ratio > worstRatio) {
                        worstRatio = ratio;
                        selectedLevel = level;
                    }
                }

                targetScheduledTime = roundToLong(targetScheduledTime / levelTimeMultiplier, HALF_UP);
            }

            if (selectedLevel == -1) {
                return null;
            }

            PrioritizedSplitRunner result = levelWaitingSplits[selectedLevel].pollFirst();
            // another taker can empty the selected level after the scan
            if (result != null) {
                return result;
            }
        }
    }

    private long getLevel0TargetTime()
    {
        long level0TargetTime = levelScheduledTime[0].get();
        double currentMultiplier = levelTimeMultiplier;

        for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.length; level++) {
            currentMultiplier /= levelTimeMultiplier;
            long levelTime = levelScheduledTime[level].get();
            level0TargetTime = Math.max(level0TargetTime, (long) (levelTime / currentMultiplier));
        }

        return level0TargetTime;
    }

    /**
     * Trino 'charges' the quanta run time to the task <i>and</i> the level it belongs to in
     * an effort to maintain the target thread utilization ratios between levels and to
     * maintain fairness within a level.
     * <p>
     * Consider an example split where a read hung for several minutes. This is either a bug
     * or a failing dependency. In either case we do not want to charge the task too much,
     * and we especially do not want to charge the level too much - i.e. cause other queries
     * in this level to starve.
     *
     * @return the new priority for the task
     */
    public Priority updatePriority(Priority oldPriority, long quantaNanos, long scheduledNanos)
    {
        int oldLevel = oldPriority.getLevel();
        int newLevel = computeLevel(scheduledNanos);

        long levelContribution = Math.min(quantaNanos, LEVEL_CONTRIBUTION_CAP);

        if (oldLevel == newLevel) {
            addLevelTime(oldLevel, levelContribution);
            return new Priority(oldLevel, oldPriority.getLevelPriority() + quantaNanos);
        }

        long remainingLevelContribution = levelContribution;
        long remainingTaskTime = quantaNanos;

        // a task normally slowly accrues scheduled time in a level and then moves to the next, but
        // if the split had a particularly long quanta, accrue time to each level as if it had run
        // in that level up to the level limit.
        for (int currentLevel = oldLevel; currentLevel < newLevel; currentLevel++) {
            long timeAccruedToLevel = Math.min(SECONDS.toNanos(LEVEL_THRESHOLD_SECONDS[currentLevel + 1] - LEVEL_THRESHOLD_SECONDS[currentLevel]), remainingLevelContribution);
            addLevelTime(currentLevel, timeAccruedToLevel);
            remainingLevelContribution -= timeAccruedToLevel;
            remainingTaskTime -= timeAccruedToLevel;
        }

        addLevelTime(newLevel, remainingLevelContribution);
        long newLevelMinPriority = getLevelMinPriority(newLevel, scheduledNanos);
        return new Priority(newLevel, newLevelMinPriority + remainingTaskTime);
    }

    public void remove(PrioritizedSplitRunner split)
    {
        checkArgument(split != null, "split is null");
        for (ConcurrentSkipListSet<PrioritizedSplitRunner> level : levelWaitingSplits) {
            if (level.remove(split)) {
                waitingSplitCount.decrementAndGet();
                // a taker can hold the permit of the removed split
                if (!waitingSplitPermits.tryAcquire()) {
                    orphanedPermits.incrementAndGet();
                }
            }
        }
    }

    public void removeAll(Collection<PrioritizedSplitRunner> splits)
    {
        for (PrioritizedSplitRunner split : splits) {
            remove(split);
        }
    }

    public long getLevelMinPriority(int level, long taskThreadUsageNanos)
    {
        levelMinPriority[level].compareAndSet(-1, taskThreadUsageNanos);
        return levelMinPriority[level].get();
    }

    public int size()
    {
        return waitingSplitCount.get();
    }

    public static int computeLevel(long threadUsageNanos)
    {
        long seconds = NANOSECONDS.toSeconds(threadUsageNanos);
        for (int level = 0; level < (LEVEL_THRESHOLD_SECONDS.length - 1); level++) {
            if (seconds < LEVEL_THRESHOLD_SECONDS[level + 1]) {
                return level;
            }
        }

        return LEVEL_THRESHOLD_SECONDS.length - 1;
    }

    @VisibleForTesting
    long getLevelScheduledTime(int level)
    {
        return levelScheduledTime[level].longValue();
    }

    @Managed
    public long getLevel0Time()
    {
        return getLevelScheduledTime(0);
    }

    @Managed
    public long getLevel1Time()
    {
        return getLevelScheduledTime(1);
    }

    @Managed
    public long getLevel2Time()
    {
        return getLevelScheduledTime(2);
    }

    @Managed
    public long getLevel3Time()
    {
        return getLevelScheduledTime(3);
    }

    @Managed
    public long getLevel4Time()
    {
        return getLevelScheduledTime(4);
    }

    @Managed
    @Nested
    public CounterStat getSelectedCountLevel0()
    {
        return selectedLevelCounters[0];
    }

    @Managed
    @Nested
    public CounterStat getSelectedCountLevel1()
    {
        return selectedLevelCounters[1];
    }

    @Managed
    @Nested
    public CounterStat getSelectedCountLevel2()
    {
        return selectedLevelCounters[2];
    }

    @Managed
    @Nested
    public CounterStat getSelectedCountLevel3()
    {
        return selectedLevelCounters[3];
    }

    @Managed
    @Nested
    public CounterStat getSelectedCountLevel4()
    {
        return selectedLevelCounters[4];
    }
}
