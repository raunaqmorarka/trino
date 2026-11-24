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

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.execution.SplitRunner;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.execution.executor.TestingTaskMemory.NO_TASK_MEMORY;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMultilevelSplitQueue
{
    private static final int TASK_COUNT = 8;
    private static final int DESTROYED_TASK_COUNT = 2;
    private static final int SPLITS_PER_TASK = 5_000;
    private static final int OFFERS_PER_SPLIT = 4;
    private static final int PRODUCER_COUNT = 4;
    private static final int TAKER_COUNT = 8;

    // concurrent offers, takes, level changes and task removal deliver each live split once per offer
    @Test
    @Timeout(60)
    public void testConcurrentOfferTakeAndRemove()
            throws Exception
    {
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        Ticker ticker = new AdvancingTicker(MILLISECONDS.toNanos(50));
        List<TimeSharingTaskHandle> taskHandles = new ArrayList<>();
        List<List<PrioritizedSplitRunner>> taskSplits = new ArrayList<>();
        List<PrioritizedSplitRunner> splits = new ArrayList<>();
        for (int task = 0; task < TASK_COUNT; task++) {
            TimeSharingTaskHandle taskHandle = createTaskHandle(splitQueue, "query" + task);
            List<PrioritizedSplitRunner> currentTaskSplits = new ArrayList<>();
            for (int split = 0; split < SPLITS_PER_TASK; split++) {
                currentTaskSplits.add(createSplit(taskHandle, splits.size() + currentTaskSplits.size(), ticker));
            }
            taskHandles.add(taskHandle);
            taskSplits.add(currentTaskSplits);
            splits.addAll(currentTaskSplits);
        }
        PrioritizedSplitRunner lastSplit = createSplit(taskHandles.getLast(), splits.size(), ticker);

        CountingTakers takers = new CountingTakers(splitQueue, splits.size() + 1, OFFERS_PER_SPLIT, new Semaphore(0));

        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger offeredSplits = new AtomicInteger();
        List<Thread> producers = new ArrayList<>();
        for (int producer = 0; producer < PRODUCER_COUNT; producer++) {
            int firstSplit = producer;
            producers.add(startDaemon(() -> {
                awaitUninterruptibly(start);
                for (int index = firstSplit; index < splits.size(); index += PRODUCER_COUNT) {
                    splitQueue.offer(splits.get(index));
                    offeredSplits.incrementAndGet();
                }
            }));
        }
        Thread destroyer = startDaemon(() -> {
            awaitUninterruptibly(start);
            while (offeredSplits.get() < splits.size() / 2) {
                Thread.onSpinWait();
            }
            for (int task = 0; task < DESTROYED_TASK_COUNT; task++) {
                taskHandles.get(task).destroy();
                splitQueue.removeAll(taskSplits.get(task));
            }
        });
        start.countDown();
        for (Thread producer : producers) {
            producer.join();
        }
        destroyer.join();

        List<PrioritizedSplitRunner> liveSplits = splits.subList(DESTROYED_TASK_COUNT * SPLITS_PER_TASK, splits.size());
        assertEventually(new Duration(30, SECONDS), () -> {
            assertThat(splitQueue.size()).isZero();
            for (PrioritizedSplitRunner split : liveSplits) {
                assertThat(takers.takeCount(split.getSplitId())).isEqualTo(OFFERS_PER_SPLIT);
            }
        });

        splitQueue.offer(lastSplit);
        assertEventually(new Duration(10, SECONDS), () -> {
            assertThat(takers.takeCount(lastSplit.getSplitId())).isEqualTo(OFFERS_PER_SPLIT);
            assertThat(splitQueue.size()).isZero();
        });

        takers.stop();
        for (int splitId = 0; splitId < DESTROYED_TASK_COUNT * SPLITS_PER_TASK; splitId++) {
            assertThat(takers.takeCount(splitId)).isLessThanOrEqualTo(OFFERS_PER_SPLIT);
        }
    }

    // splits move between levels while few splits wait
    @Test
    @Timeout(60)
    public void testLevelChangesWithFewWaitingSplits()
            throws Exception
    {
        int taskCount = 5_000;
        int splitsPerTask = 2;
        int offersPerSplit = 30;
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        Ticker ticker = new AdvancingTicker(SECONDS.toNanos(1));
        Semaphore splitsInFlight = new Semaphore(TAKER_COUNT * 2);
        CountingTakers takers = new CountingTakers(splitQueue, taskCount * splitsPerTask, offersPerSplit, splitsInFlight);
        for (int task = 0; task < taskCount; task++) {
            TimeSharingTaskHandle taskHandle = createTaskHandle(splitQueue, "query" + task);
            for (int split = 0; split < splitsPerTask; split++) {
                splitsInFlight.acquire();
                splitQueue.offer(createSplit(taskHandle, task * splitsPerTask + split, ticker));
            }
        }

        assertEventually(new Duration(30, SECONDS), () -> {
            assertThat(splitQueue.size()).isZero();
            for (int splitId = 0; splitId < taskCount * splitsPerTask; splitId++) {
                assertThat(takers.takeCount(splitId)).isEqualTo(offersPerSplit);
            }
        });
        takers.stop();
    }

    // removing a split that a waiting taker holds the permit for leaves the queue usable
    @Test
    @Timeout(60)
    public void testRemoveWhileTakersWait()
            throws Exception
    {
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        Set<PrioritizedSplitRunner> takenSplits = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicateTakes = new AtomicInteger();
        List<Thread> takers = new ArrayList<>();
        for (int taker = 0; taker < TAKER_COUNT; taker++) {
            takers.add(startDaemon(() -> {
                try {
                    while (true) {
                        if (!takenSplits.add(splitQueue.take())) {
                            duplicateTakes.incrementAndGet();
                        }
                    }
                }
                catch (InterruptedException _) {
                    // takers stop on interrupt
                }
            }));
        }

        for (int split = 0; split < 20_000; split++) {
            TimeSharingTaskHandle taskHandle = createTaskHandle(splitQueue, "query" + split);
            PrioritizedSplitRunner removedSplit = createSplit(taskHandle, split, Ticker.systemTicker());
            splitQueue.offer(removedSplit);
            taskHandle.destroy();
            splitQueue.remove(removedSplit);
        }

        PrioritizedSplitRunner lastSplit = createSplit(createTaskHandle(splitQueue, "last"), 20_000, Ticker.systemTicker());
        splitQueue.offer(lastSplit);
        assertEventually(new Duration(10, SECONDS), () -> {
            assertThat(takenSplits).contains(lastSplit);
            assertThat(splitQueue.size()).isZero();
        });

        stopTakers(takers);
        assertThat(duplicateTakes.get()).isZero();
    }

    // a split of a destroyed task that changed level is dropped together with its permit
    @Test
    @Timeout(60)
    public void testDestroyedSplitWithChangedLevelIsDropped()
            throws Exception
    {
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        TimeSharingTaskHandle destroyedTaskHandle = createTaskHandle(splitQueue, "destroyed");
        PrioritizedSplitRunner destroyedSplit = createSplit(destroyedTaskHandle, 0, Ticker.systemTicker());
        splitQueue.offer(destroyedSplit);
        destroyedTaskHandle.addScheduledNanos(SECONDS.toNanos(2));
        destroyedTaskHandle.destroy();

        TimeSharingTaskHandle liveTaskHandle = createTaskHandle(splitQueue, "live");
        liveTaskHandle.addScheduledNanos(MILLISECONDS.toNanos(500));
        PrioritizedSplitRunner liveSplit = createSplit(liveTaskHandle, 1, Ticker.systemTicker());
        splitQueue.offer(liveSplit);

        assertThat(splitQueue.take()).isSameAs(liveSplit);
        assertThat(splitQueue.size()).isZero();

        Thread taker = startDaemon(() -> {
            try {
                splitQueue.take();
            }
            catch (InterruptedException _) {
                // the taker stops on interrupt
            }
        });
        assertEventually(new Duration(10, SECONDS), () -> assertThat(taker.getState()).isEqualTo(Thread.State.WAITING));
        stopTakers(ImmutableList.of(taker));
    }

    private static TimeSharingTaskHandle createTaskHandle(MultilevelSplitQueue splitQueue, String queryId)
    {
        return new TimeSharingTaskHandle(new TaskId(new StageId(queryId, 0), 0, 0), splitQueue, () -> 1, 1, new Duration(1, SECONDS), OptionalInt.empty(), NO_TASK_MEMORY);
    }

    private static PrioritizedSplitRunner createSplit(TimeSharingTaskHandle taskHandle, int splitId, Ticker ticker)
    {
        return new PrioritizedSplitRunner(
                taskHandle,
                splitId,
                true,
                new InstantSplitRunner(),
                Span.getInvalid(),
                noopTracer(),
                ticker,
                new CounterStat(),
                new CounterStat(),
                new TimeStat(),
                new TimeStat());
    }

    private static Thread startDaemon(Runnable runnable)
    {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void awaitUninterruptibly(CountDownLatch latch)
    {
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    // a taker that spins on a permit without a split ignores the interrupt and stays alive
    private static void stopTakers(List<Thread> takers)
            throws InterruptedException
    {
        for (Thread taker : takers) {
            taker.interrupt();
        }
        for (Thread taker : takers) {
            taker.join(SECONDS.toMillis(10));
            assertThat(taker.isAlive()).isFalse();
        }
    }

    // takers run each split once per offer and count takes per split id
    private static class CountingTakers
    {
        private final MultilevelSplitQueue splitQueue;
        private final int offersPerSplit;
        private final Semaphore finishedSplits;
        private final AtomicIntegerArray takeCounts;
        private final AtomicIntegerArray heldSplits;
        private final AtomicInteger duplicateTakes = new AtomicInteger();
        private final List<Thread> threads = new ArrayList<>();

        private CountingTakers(MultilevelSplitQueue splitQueue, int splitCount, int offersPerSplit, Semaphore finishedSplits)
        {
            this.splitQueue = splitQueue;
            this.offersPerSplit = offersPerSplit;
            this.finishedSplits = finishedSplits;
            this.takeCounts = new AtomicIntegerArray(splitCount);
            this.heldSplits = new AtomicIntegerArray(splitCount);
            for (int taker = 0; taker < TAKER_COUNT; taker++) {
                threads.add(startDaemon(this::takeSplits));
            }
        }

        private void takeSplits()
        {
            try {
                while (true) {
                    PrioritizedSplitRunner split = splitQueue.take();
                    int splitId = split.getSplitId();
                    if (!heldSplits.compareAndSet(splitId, 0, 1)) {
                        duplicateTakes.incrementAndGet();
                    }
                    int takeCount = takeCounts.incrementAndGet(splitId);
                    split.process();
                    if (takeCount % 2 == 0) {
                        split.resetLevelPriority();
                    }
                    heldSplits.set(splitId, 0);
                    if (takeCount < offersPerSplit) {
                        splitQueue.offer(split);
                    }
                    else {
                        finishedSplits.release();
                    }
                }
            }
            catch (InterruptedException _) {
                // takers stop on interrupt
            }
        }

        private int takeCount(int splitId)
        {
            return takeCounts.get(splitId);
        }

        private void stop()
                throws InterruptedException
        {
            stopTakers(threads);
            assertThat(duplicateTakes.get()).isZero();
        }
    }

    private static class AdvancingTicker
            extends Ticker
    {
        private final AtomicLong nanos = new AtomicLong();
        private final long stepNanos;

        private AdvancingTicker(long stepNanos)
        {
            this.stepNanos = stepNanos;
        }

        @Override
        public long read()
        {
            return nanos.addAndGet(stepNanos);
        }
    }

    private static class InstantSplitRunner
            implements SplitRunner
    {
        @Override
        public int getPipelineId()
        {
            return 0;
        }

        @Override
        public Span getPipelineSpan()
        {
            return Span.getInvalid();
        }

        @Override
        public boolean isFinished()
        {
            return false;
        }

        @Override
        public ListenableFuture<Void> processFor(Duration duration)
        {
            return immediateVoidFuture();
        }

        @Override
        public String getInfo()
        {
            return "instant";
        }

        @Override
        public void close() {}
    }
}
