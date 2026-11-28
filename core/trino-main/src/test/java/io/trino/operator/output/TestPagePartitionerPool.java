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
package io.trino.operator.output;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.StateMachine;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.OutputBufferInfo;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.output.PartitionedOutputOperator.PartitionedOutputOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.block.DictionaryBlock;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import io.trino.type.BlockTypeOperators;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.execution.buffer.CompressionCodec.LZ4;
import static io.trino.execution.buffer.PagesSerdeUtil.getSerializedPagePositionCount;
import static io.trino.execution.buffer.TestingPagesSerdes.createTestingPagesSerdeFactory;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPagePartitionerPool
{
    private ScheduledExecutorService driverYieldExecutor;

    @BeforeAll
    public void setUp()
    {
        driverYieldExecutor = newScheduledThreadPool(0, threadsNamed("TestPagePartitionerPool-driver-yield-%s"));
    }

    @AfterAll
    public void destroy()
    {
        driverYieldExecutor.shutdown();
    }

    @Test
    public void testBuffersReusedAcrossSplits()
            throws Exception
    {
        Page split = new Page(createLongsBlock(1));
        // A page can fit one split, but not two.
        DataSize maxPageSize = DataSize.ofBytes(split.getSizeInBytes() + 1);

        OutputBufferMock outputBuffer = new OutputBufferMock();
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        PartitionedOutputOperatorFactory factory = createFactory(maxPageSize, outputBuffer, memoryContext);

        assertThat(memoryContext.getBytes()).isEqualTo(0);
        // first split, too small for a flush
        long unusedPartitionerBytes = processSplitsConcurrently(factory, memoryContext, split);
        assertThat(outputBuffer.totalEnqueuedPageCount()).isEqualTo(0);
        long bufferedPartitionerBytes = memoryContext.getBytes();

        // second split makes the split partitioner buffer full so the split is flushed
        processSplitsConcurrently(factory, memoryContext, split);
        assertThat(outputBuffer.totalEnqueuedPageCount()).isEqualTo(1);
        // The flushed partitioner retains its page builder for reuse.
        long flushedPartitionerBytes = memoryContext.getBytes();
        assertThat(flushedPartitionerBytes).isGreaterThan(unusedPartitionerBytes);
        assertThat(bufferedPartitionerBytes).isGreaterThanOrEqualTo(flushedPartitionerBytes + split.getSizeInBytes());

        // One partitioner is reused and one is new. Neither flushes after a single split.
        long retainedBeforeInput = processSplitsConcurrently(factory, memoryContext, split, split);
        assertThat(retainedBeforeInput).isEqualTo(flushedPartitionerBytes + unusedPartitionerBytes);
        assertThat(outputBuffer.totalEnqueuedPageCount()).isEqualTo(1);
        assertThat(memoryContext.getBytes()).isGreaterThanOrEqualTo(2 * (flushedPartitionerBytes + split.getSizeInBytes()));

        // another pair of splits should flush both buffers
        processSplitsConcurrently(factory, memoryContext, split, split);
        assertThat(outputBuffer.totalEnqueuedPageCount()).isEqualTo(3);
        assertThat(memoryContext.getBytes()).isEqualTo(2 * flushedPartitionerBytes);

        // max free buffers is set to 2 so 2 buffers are retained and the other 2 merge into them, filling both
        processSplitsConcurrently(factory, memoryContext, split, split, split, split);
        assertThat(outputBuffer.totalEnqueuedPageCount()).isEqualTo(5);
        assertThat(memoryContext.getBytes()).isEqualTo(2 * flushedPartitionerBytes);

        // another pair of splits stays buffered in the free partitioners
        processSplitsConcurrently(factory, memoryContext, split, split);
        assertThat(outputBuffer.totalEnqueuedPageCount()).isEqualTo(5);
        assertThat(memoryContext.getBytes()).isGreaterThanOrEqualTo(2 * (flushedPartitionerBytes + split.getSizeInBytes()));

        try (Operator operator = factory.createOperator(driverContext())) {
            // noMoreOperators keeps the free buffer while an operator is outstanding
            factory.noMoreOperators();
            assertThat(outputBuffer.totalEnqueuedPageCount()).isEqualTo(5);

            // the last operator merges its buffer into the free one, which fills and is flushed
            operator.finish();
            assertThat(outputBuffer.totalEnqueuedPageCount()).isEqualTo(6);
            assertThat(outputBuffer.totalEnqueuedPositionCount()).isEqualTo(12);
            // pool is closed, all operators are finished/flushed, the retained memory should be 0
            assertThat(memoryContext.getBytes()).isEqualTo(0);
        }
    }

    @Test
    public void testBuffersMergedAfterNoMoreOperators()
    {
        Page split = new Page(createLongsBlock(1));
        // A page can fit one split, but not two.
        DataSize maxPageSize = DataSize.ofBytes(split.getSizeInBytes() + 1);
        OutputBufferMock outputBuffer = new OutputBufferMock();
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        PartitionedOutputOperatorFactory factory = createFactory(maxPageSize, outputBuffer, memoryContext);

        List<Operator> operators = createOperators(factory, 4);
        factory.noMoreOperators();
        for (Operator operator : operators) {
            operator.addInput(split);
            operator.finish();
        }
        assertThat(outputBuffer.totalEnqueuedPageCount()).isEqualTo(2);
        assertThat(outputBuffer.totalEnqueuedPositionCount()).isEqualTo(4);
        assertThat(memoryContext.getBytes()).isZero();
    }

    @Test
    public void testConcurrentReleaseAndNoMoreOperators()
            throws Exception
    {
        Page split = new Page(createLongsBlock(1));
        DataSize maxPageSize = DataSize.ofBytes(split.getSizeInBytes() + 1);
        int operatorCount = 8;
        ExecutorService executor = newFixedThreadPool(operatorCount + 1, threadsNamed("TestPagePartitionerPool-release-%s"));
        try {
            for (int iteration = 0; iteration < 100; iteration++) {
                OutputBufferMock outputBuffer = new OutputBufferMock();
                AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
                PartitionedOutputOperatorFactory factory = createFactory(maxPageSize, outputBuffer, memoryContext);
                List<Operator> operators = createOperators(factory, operatorCount);

                CyclicBarrier barrier = new CyclicBarrier(operatorCount + 1);
                List<Future<Object>> finished = operators.stream()
                        .map(operator -> executor.submit(() -> {
                            barrier.await();
                            operator.addInput(split);
                            operator.finish();
                            return null;
                        }))
                        .collect(toImmutableList());
                Future<Object> closed = executor.submit(() -> {
                    barrier.await();
                    factory.noMoreOperators();
                    return null;
                });
                closed.get(10, SECONDS);
                for (Future<Object> future : finished) {
                    future.get(10, SECONDS);
                }

                assertThat(outputBuffer.totalEnqueuedPositionCount()).isEqualTo(operatorCount);
                assertThat(memoryContext.getBytes()).isZero();
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testMergedPartitionerClosedWhenFreePartitionersFull()
            throws Exception
    {
        Page split = new Page(createLongsBlock(1));
        // A page can fit one split, but not two.
        DataSize maxPageSize = DataSize.ofBytes(split.getSizeInBytes() + 1);
        AggregatedMemoryContext referenceMemoryContext = newSimpleAggregatedMemoryContext();
        processSplitsConcurrently(createFactory(maxPageSize, new OutputBufferMock(), referenceMemoryContext), referenceMemoryContext, split);
        long bufferedPartitionerBytes = referenceMemoryContext.getBytes();

        CountDownLatch enqueueStarted = new CountDownLatch(1);
        CountDownLatch enqueueUnblocked = new CountDownLatch(1);
        OutputBufferMock outputBuffer = new OutputBufferMock()
        {
            @Override
            public void enqueue(int partition, List<Slice> pages)
            {
                enqueueStarted.countDown();
                awaitUninterruptibly(enqueueUnblocked);
                super.enqueue(partition, pages);
            }
        };
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        PartitionedOutputOperatorFactory factory = createFactory(maxPageSize, outputBuffer, memoryContext);
        List<Operator> operators = createOperators(factory, 4);
        for (Operator operator : operators) {
            operator.addInput(split);
        }
        // max free buffers is set to 2, so the first two operators fill the free partitioners
        operators.get(0).finish();
        operators.get(1).finish();

        ExecutorService executor = newSingleThreadExecutor(threadsNamed("TestPagePartitionerPool-merge-%s"));
        try {
            // the third operator merges into a free partitioner, which fills and blocks on enqueue
            Future<?> merged = executor.submit(operators.get(2)::finish);
            assertThat(enqueueStarted.await(10, SECONDS)).isTrue();
            // the fourth operator takes the free slot left by the merge
            operators.get(3).finish();
            enqueueUnblocked.countDown();
            merged.get(10, SECONDS);
        }
        finally {
            enqueueUnblocked.countDown();
            executor.shutdownNow();
        }
        // the merged partitioner is closed, so only the two buffered partitioners stay free
        assertThat(memoryContext.getBytes()).isEqualTo(2 * bufferedPartitionerBytes);

        factory.noMoreOperators();
        assertThat(outputBuffer.totalEnqueuedPositionCount()).isEqualTo(4);
        assertThat(memoryContext.getBytes()).isZero();
    }

    @Test
    public void testMemoryReleasedOnFailure()
    {
        Page split = new Page(createLongsBlock(1));
        // A page can fit one split, but not two.
        DataSize maxPageSize = DataSize.ofBytes(split.getSizeInBytes() + 1);
        RuntimeException exception = new RuntimeException();
        OutputBufferMock outputBuffer = new OutputBufferMock()
        {
            @Override
            public void enqueue(int partition, List<Slice> pages)
            {
                throw exception;
            }
        };
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        PartitionedOutputOperatorFactory factory = createFactory(maxPageSize, outputBuffer, memoryContext);

        long initialRetainedBytesOneOperator = processSplitsConcurrently(factory, memoryContext, split);
        assertThat(memoryContext.getBytes()).isGreaterThanOrEqualTo(initialRetainedBytesOneOperator + split.getSizeInBytes());

        assertThatThrownBy(factory::noMoreOperators).isEqualTo(exception);
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }

    @Test
    public void testMemoryReleasedOnReleaseFailure()
    {
        RuntimeException exception = new RuntimeException();
        OutputBufferMock outputBuffer = new OutputBufferMock()
        {
            @Override
            public void enqueue(int partition, List<Slice> pages)
            {
                throw exception;
            }
        };
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        PartitionedOutputOperatorFactory factory = createFactory(DataSize.of(1, MEGABYTE), outputBuffer, memoryContext);
        Operator operator = factory.createOperator(driverContext());
        // repeated dictionary ids make release flush the buffered page
        operator.addInput(new Page(DictionaryBlock.create(4, createLongsBlock(0, 1), new int[] {0, 1, 0, 1})));
        assertThat(memoryContext.getBytes()).isPositive();

        assertThatThrownBy(operator::finish).isSameAs(exception);
        assertThat(memoryContext.getBytes()).isZero();
    }

    @Test
    public void testMemoryReleasedOnReleaseFailureAfterNoMoreOperators()
    {
        RuntimeException exception = new RuntimeException();
        OutputBufferMock outputBuffer = new OutputBufferMock()
        {
            @Override
            public void enqueue(int partition, List<Slice> pages)
            {
                throw exception;
            }
        };
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        PartitionedOutputOperatorFactory factory = createFactory(DataSize.of(1, MEGABYTE), outputBuffer, memoryContext);
        List<Operator> operators = createOperators(factory, 2);
        factory.noMoreOperators();

        // the first operator's buffer stays in the pool until the last operator is released
        operators.getFirst().addInput(new Page(createLongsBlock(0)));
        operators.getFirst().finish();
        assertThat(memoryContext.getBytes()).isPositive();

        // repeated dictionary ids make release flush the buffered page
        Operator operator = operators.getLast();
        operator.addInput(new Page(DictionaryBlock.create(4, createLongsBlock(0, 1), new int[] {0, 1, 0, 1})));
        assertThatThrownBy(operator::finish).isSameAs(exception);
        assertThat(memoryContext.getBytes()).isZero();
    }

    @Test
    public void testMemoryReleasedOnMergeFailure()
    {
        Page split = new Page(createLongsBlock(1));
        // A page can fit one split, but not two.
        DataSize maxPageSize = DataSize.ofBytes(split.getSizeInBytes() + 1);
        RuntimeException exception = new RuntimeException();
        OutputBufferMock outputBuffer = new OutputBufferMock()
        {
            @Override
            public void enqueue(int partition, List<Slice> pages)
            {
                throw exception;
            }
        };
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        PartitionedOutputOperatorFactory factory = createFactory(maxPageSize, outputBuffer, memoryContext);
        List<Operator> operators = createOperators(factory, 2);
        factory.noMoreOperators();

        operators.getFirst().addInput(split);
        operators.getFirst().finish();
        // merging the second buffer fills the first one, whose flush fails
        operators.getLast().addInput(split);
        assertThatThrownBy(operators.getLast()::finish).isSameAs(exception);
        assertThat(memoryContext.getBytes()).isZero();
    }

    private static PartitionedOutputOperatorFactory createFactory(DataSize maxPageSize, OutputBufferMock outputBuffer, AggregatedMemoryContext memoryContext)
    {
        // Partition 0 stays unused across checkouts.
        BucketPartitionFunction partitionFunction = new BucketPartitionFunction((_, _) -> 1, new int[] {0, 1});
        return new PartitionedOutputOperatorFactory(
                0,
                new PlanNodeId("0"),
                ImmutableList.of(BIGINT),
                Function.identity(),
                partitionFunction,
                ImmutableList.of(0),
                ImmutableList.of(),
                false,
                OptionalInt.empty(),
                outputBuffer,
                createTestingPagesSerdeFactory(LZ4),
                DataSize.ofBytes(maxPageSize.toBytes() * partitionFunction.partitionCount()),
                new PositionsAppenderFactory(new BlockTypeOperators()),
                Optional.empty(),
                memoryContext,
                2,
                Optional.empty());
    }

    private long processSplitsConcurrently(PartitionedOutputOperatorFactory factory, AggregatedMemoryContext memoryContext, Page... splits)
    {
        List<Operator> operators = Stream.of(splits)
                .map(_ -> factory.createOperator(driverContext()))
                .collect(toImmutableList());

        long initialRetainedBytes = memoryContext.getBytes();
        for (int i = 0; i < operators.size(); i++) {
            operators.get(i).addInput(splits[i]);
        }
        operators.forEach(Operator::finish);
        return initialRetainedBytes;
    }

    private List<Operator> createOperators(PartitionedOutputOperatorFactory factory, int count)
    {
        return IntStream.range(0, count)
                .mapToObj(_ -> factory.createOperator(driverContext()))
                .collect(toImmutableList());
    }

    private DriverContext driverContext()
    {
        return TestingTaskContext.builder(directExecutor(), driverYieldExecutor, TEST_SESSION)
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private static class OutputBufferMock
            implements OutputBuffer
    {
        Map<Integer, Integer> partitionBufferPages = new HashMap<>();
        private int enqueuedPositionCount;

        @Override
        public boolean usesExternalStorage()
        {
            return false;
        }

        public synchronized int totalEnqueuedPageCount()
        {
            return partitionBufferPages.values().stream().mapToInt(Integer::intValue).sum();
        }

        public synchronized int totalEnqueuedPositionCount()
        {
            return enqueuedPositionCount;
        }

        @Override
        public synchronized void enqueue(int partition, List<Slice> pages)
        {
            partitionBufferPages.compute(partition, (_, value) -> value == null ? pages.size() : value + pages.size());
            for (Slice page : pages) {
                enqueuedPositionCount += getSerializedPagePositionCount(page);
            }
        }

        @Override
        public OutputBufferInfo getInfo()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferState getState()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getUtilization()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputBufferStatus getStatus()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addStateChangeListener(StateMachine.StateChangeListener<BufferState> stateChangeListener)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setOutputBuffers(OutputBuffers newOutputBuffers)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<BufferResult> get(OutputBufferId bufferId, long token, DataSize maxSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void acknowledge(OutputBufferId bufferId, long token)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void destroy(OutputBufferId bufferId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<Void> isFull()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enqueue(List<Slice> pages)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setNoMorePages()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void destroy()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void abort()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPeakMemoryUsage()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Throwable> getFailureCause()
        {
            throw new UnsupportedOperationException();
        }
    }
}
