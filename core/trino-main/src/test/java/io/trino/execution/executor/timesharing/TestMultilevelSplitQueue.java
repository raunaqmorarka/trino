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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import io.airlift.testing.TestingTicker;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.execution.executor.TestingTaskMemory.NO_TASK_MEMORY;
import static io.trino.execution.executor.timesharing.MultilevelSplitQueue.LEVEL_THRESHOLD_SECONDS;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestMultilevelSplitQueue
{
    @Test
    @Timeout(30)
    void testTakeOfferedSplits()
            throws Exception
    {
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        TimeSharingTaskHandle taskHandle = createTaskHandle(splitQueue, "test0");
        PrioritizedSplitRunner first = createSplit(taskHandle, 0);
        PrioritizedSplitRunner second = createSplit(taskHandle, 1);

        splitQueue.offer(first);
        splitQueue.offer(second);
        assertThat(splitQueue.size()).isEqualTo(2);

        assertThat(ImmutableList.of(splitQueue.take(), splitQueue.take())).containsExactlyInAnyOrder(first, second);
        assertThat(splitQueue.size()).isEqualTo(0);
    }

    @Test
    @Timeout(30)
    void testRemovedSplitIsNotTaken()
            throws Exception
    {
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        TimeSharingTaskHandle taskHandle = createTaskHandle(splitQueue, "test0");
        PrioritizedSplitRunner removed = createSplit(taskHandle, 0);
        PrioritizedSplitRunner kept = createSplit(taskHandle, 1);

        splitQueue.offer(removed);
        splitQueue.remove(removed);
        assertThat(splitQueue.size()).isEqualTo(0);

        splitQueue.offer(kept);
        assertThat(splitQueue.take()).isSameAs(kept);
        assertThat(splitQueue.size()).isEqualTo(0);
    }

    @Test
    @Timeout(30)
    void testRemoveAll()
            throws Exception
    {
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        TimeSharingTaskHandle taskHandle = createTaskHandle(splitQueue, "test0");
        PrioritizedSplitRunner first = createSplit(taskHandle, 0);
        PrioritizedSplitRunner second = createSplit(taskHandle, 1);
        PrioritizedSplitRunner kept = createSplit(taskHandle, 2);

        splitQueue.offer(first);
        splitQueue.offer(second);
        splitQueue.offer(kept);
        splitQueue.removeAll(ImmutableList.of(first, second));
        assertThat(splitQueue.size()).isEqualTo(1);

        assertThat(splitQueue.take()).isSameAs(kept);
        assertThat(splitQueue.size()).isEqualTo(0);
    }

    @Test
    @Timeout(30)
    void testSplitOfDestroyedTaskIsDiscarded()
            throws Exception
    {
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        TimeSharingTaskHandle destroyedTask = createTaskHandle(splitQueue, "test0");
        TimeSharingTaskHandle runningTask = createTaskHandle(splitQueue, "test1");
        PrioritizedSplitRunner offeredBeforeDestroy = createSplit(destroyedTask, 0);
        PrioritizedSplitRunner offeredAfterDestroy = createSplit(destroyedTask, 1);
        PrioritizedSplitRunner kept = createSplit(runningTask, 0);

        splitQueue.offer(offeredBeforeDestroy);
        destroyedTask.destroy();
        splitQueue.offer(offeredAfterDestroy);
        splitQueue.offer(kept);

        assertThat(splitQueue.take()).isSameAs(kept);
        assertThat(splitQueue.size()).isEqualTo(0);
    }

    @Test
    @Timeout(30)
    void testTakeWaitsPastPermitOfRemovedSplit()
            throws Exception
    {
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        TimeSharingTaskHandle taskHandle = createTaskHandle(splitQueue, "test0");
        PrioritizedSplitRunner removed = createSplit(taskHandle, 0);
        PrioritizedSplitRunner kept = createSplit(taskHandle, 1);

        splitQueue.offer(removed);
        splitQueue.remove(removed);

        FutureTask<PrioritizedSplitRunner> take = new FutureTask<>(splitQueue::take);
        Thread taker = new Thread(take, "test-split-queue-taker");
        taker.setDaemon(true);
        taker.start();
        while (taker.getState() != Thread.State.WAITING) {
            Thread.sleep(10);
        }

        splitQueue.offer(kept);
        assertThat(take.get()).isSameAs(kept);
        assertThat(splitQueue.size()).isEqualTo(0);
    }

    @Test
    @Timeout(30)
    void testSplitOfDestroyedTaskWhoseLevelChangedIsDiscarded()
            throws Exception
    {
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        TimeSharingTaskHandle destroyedTask = createTaskHandle(splitQueue, "test0");
        TimeSharingTaskHandle runningTask = createTaskHandle(splitQueue, "test1");
        PrioritizedSplitRunner first = createSplit(runningTask, 0);
        PrioritizedSplitRunner discarded = createSplit(destroyedTask, 0);
        PrioritizedSplitRunner kept = createSplit(runningTask, 1);

        splitQueue.offer(first);
        splitQueue.offer(discarded);
        splitQueue.offer(kept);
        assertThat(splitQueue.take()).isSameAs(first);

        destroyedTask.addScheduledNanos(SECONDS.toNanos(LEVEL_THRESHOLD_SECONDS[1]));
        destroyedTask.destroy();

        assertThat(splitQueue.take()).isSameAs(kept);
        assertThat(splitQueue.size()).isEqualTo(0);
    }

    @Test
    @Timeout(30)
    void testTakeSplitWhoseLevelChanged()
            throws Exception
    {
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        TimeSharingTaskHandle taskHandle = createTaskHandle(splitQueue, "test0");
        PrioritizedSplitRunner split = createSplit(taskHandle, 0);

        splitQueue.offer(split);
        taskHandle.addScheduledNanos(SECONDS.toNanos(LEVEL_THRESHOLD_SECONDS[1]));

        assertThat(splitQueue.take()).isSameAs(split);
        assertThat(split.getPriority().getLevel()).isEqualTo(1);
        assertThat(splitQueue.size()).isEqualTo(0);
    }

    @Test
    @Timeout(60)
    void testConcurrentOfferAndTake()
            throws Exception
    {
        int threads = 4;
        int splitsPerThread = 5_000;
        MultilevelSplitQueue splitQueue = new MultilevelSplitQueue(2);
        TimeSharingTaskHandle taskHandle = createTaskHandle(splitQueue, "test0");
        Set<PrioritizedSplitRunner> taken = ConcurrentHashMap.newKeySet();
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-split-queue-%s"));
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int thread = 0; thread < threads; thread++) {
                int firstSplitId = thread * splitsPerThread;
                futures.add(executor.submit(() -> {
                    for (int split = 0; split < splitsPerThread; split++) {
                        splitQueue.offer(createSplit(taskHandle, firstSplitId + split));
                    }
                }));
                futures.add(executor.submit(() -> {
                    for (int split = 0; split < splitsPerThread; split++) {
                        assertThat(taken.add(splitQueue.take())).isTrue();
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        }
        finally {
            executor.shutdownNow();
        }

        assertThat(taken).hasSize(threads * splitsPerThread);
        assertThat(splitQueue.size()).isEqualTo(0);
    }

    private static TimeSharingTaskHandle createTaskHandle(MultilevelSplitQueue splitQueue, String queryId)
    {
        return new TimeSharingTaskHandle(new TaskId(new StageId(queryId, 0), 0, 0), splitQueue, () -> 1, 1, new Duration(1, SECONDS), OptionalInt.empty(), NO_TASK_MEMORY);
    }

    private static PrioritizedSplitRunner createSplit(TimeSharingTaskHandle taskHandle, int splitId)
    {
        return new PrioritizedSplitRunner(
                taskHandle,
                splitId,
                true,
                new TestingSplitRunner(),
                Span.getInvalid(),
                noopTracer(),
                new TestingTicker(),
                new CounterStat(),
                new CounterStat(),
                new TimeStat(),
                new TimeStat());
    }

    private static class TestingSplitRunner
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
            return "testing split";
        }

        @Override
        public void close() {}
    }
}
