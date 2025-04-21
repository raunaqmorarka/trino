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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.SequencePageBuilder;
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.Split;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import io.trino.operator.project.CursorProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.gen.CursorProcessorCompiler;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.gen.columnar.ColumnarFilterCompiler;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingTaskContext;
import io.trino.transaction.TestingTransactionManager;
import io.trino.type.LikePattern;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSplit.createLocalSplit;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 10, time = 2)
public class BenchmarkScanFilterAndProjectOperator
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final TestingTransactionManager TRANSACTION_MANAGER = new TestingTransactionManager();
    private static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder()
            .withTransactionManager(TRANSACTION_MANAGER)
            .build();

    private static final Map<String, Type> TYPE_MAP = ImmutableMap.of("bigint", BIGINT, "varchar", VARCHAR);

    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();

    private static final int TOTAL_POSITIONS = 1_000_000;
    private static final DataSize FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE = DataSize.of(500, KILOBYTE);
    private static final int FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT = 256;

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction MODULUS_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MODULUS, ImmutableList.of(BIGINT, BIGINT));

    @State(Thread)
    public static class Context
    {
        private final Map<Symbol, Integer> sourceLayout = new HashMap<>();

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;

        @Param({"1024"})
        int positionsPerPage = 1024;

        @Param({"8"})
        int columnCount = 8;

        @Param({"varchar"})
        String type = "varchar";

        @Param({"true", "false"})
        boolean useRecordCursor;

        @Param({"true", "false"})
        boolean columnarFilterEvaluationEnabled;

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

            Type type = TYPE_MAP.get(this.type);

            for (int i = 0; i < columnCount; i++) {
                Symbol symbol = new Symbol(type, type.getDisplayName().toLowerCase(ENGLISH) + i);
                sourceLayout.put(symbol, i);
            }

            List<RowExpression> projections = getProjections(type);
            List<Type> types = projections.stream().map(RowExpression::type).collect(toList());
            List<ColumnHandle> columnHandles = IntStream.range(0, columnCount)
                    .mapToObj(i -> new TestingColumnHandle(Integer.toString(i)))
                    .collect(toImmutableList());

            CursorProcessorCompiler cursorProcessorCompiler = new CursorProcessorCompiler(PLANNER_CONTEXT.getFunctionManager());
            PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(PLANNER_CONTEXT.getFunctionManager(), 0);
            ColumnarFilterCompiler compiler = new ColumnarFilterCompiler(PLANNER_CONTEXT.getFunctionManager(), 0);
            PageProcessor pageProcessor = new ExpressionCompiler(cursorProcessorCompiler, pageFunctionCompiler, compiler)
                    .compilePageProcessor(columnarFilterEvaluationEnabled, Optional.of(getFilter(type)), Optional.empty(), projections, Optional.empty(), OptionalInt.empty()).apply(DynamicFilter.EMPTY);
            CursorProcessor cursorProcessor = new ExpressionCompiler(cursorProcessorCompiler, pageFunctionCompiler, compiler).compileCursorProcessor(Optional.of(getFilter(type)), projections, "key").get();

            createTaskContext();
            createScanFilterAndProjectOperatorFactories(createInputPages(types), pageProcessor, cursorProcessor, columnHandles, types);
        }

        @TearDown
        public void cleanup()
        {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }

        private void createScanFilterAndProjectOperatorFactories(List<Page> inputPages, PageProcessor pageProcessor, CursorProcessor cursorProcessor, List<ColumnHandle> columnHandles, List<Type> types)
        {
            operatorFactory = new ScanFilterAndProjectOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    new PlanNodeId("test_source"),
                    (catalog) -> (session, split, table, columns, dynamicFilter) ->
                            useRecordCursor ? new RecordPageSource(types, new PageRecordCursor(types, inputPages)) : new FixedPageSource(inputPages),
                    () -> cursorProcessor,
                    (_) -> pageProcessor,
                    TEST_TABLE_HANDLE,
                    columnHandles,
                    DynamicFilter.EMPTY,
                    types,
                    FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE,
                    FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT);
        }

        public TaskContext createTaskContext()
        {
            return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.of(2, GIGABYTE));
        }

        public OperatorFactory getOperatorFactory()
        {
            return operatorFactory;
        }

        private List<Page> createInputPages(List<Type> types)
        {
            ImmutableList.Builder<Page> inputPagesBuilder = ImmutableList.builder();
            for (int i = 0; i < TOTAL_POSITIONS / positionsPerPage; ++i) {
                inputPagesBuilder.add(createPage(types, positionsPerPage));
            }
            return inputPagesBuilder.build();
        }

        private RowExpression getFilter(Type type)
        {
            if (type == VARCHAR) {
                return call(
                        FUNCTION_RESOLUTION.resolveFunction("$like", fromTypes(VARCHAR, LIKE_PATTERN)),
                        field(0, VARCHAR),
                        constant(LikePattern.compile("%23", Optional.empty()), LIKE_PATTERN));
            }
            if (type == BIGINT) {
                return rowExpression(new Comparison(EQUAL, new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(INTEGER, "bigint0"), new Constant(INTEGER, 2L))), new Constant(INTEGER, 0L)));
            }
            throw new IllegalArgumentException("filter not supported for type : " + type);
        }

        private List<RowExpression> getProjections(Type type)
        {
            ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
            for (int i = 0; i < columnCount; i++) {
                builder.add(new InputReferenceExpression(i, type));
            }
            return builder.build();
        }

        private RowExpression rowExpression(Expression expression)
        {
            return SqlToRowExpressionTranslator.translate(
                    expression,
                    sourceLayout,
                    PLANNER_CONTEXT.getMetadata(),
                    PLANNER_CONTEXT.getTypeManager());
        }

        private static Page createPage(List<? extends Type> types, int positions)
        {
            return SequencePageBuilder.createSequencePage(types, positions);
        }
    }

    @Benchmark
    public double benchmarkColumnOriented(Context context)
    {
        DriverContext driverContext = context.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        SourceOperator operator = (SourceOperator) context.getOperatorFactory().createOperator(driverContext);

        long outputRows = 0;
        operator.addSplit(new Split(TEST_CATALOG_HANDLE, createLocalSplit()));
        operator.noMoreSplits();

        while (!operator.isFinished()) {
            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputRows += outputPage.getPositionCount();
            }
        }

        return outputRows * 100.0 / TOTAL_POSITIONS;
    }

    @Test
    public void testBenchmark()
    {
        Context context = new Context();
        context.setup();
        System.out.println(benchmarkColumnOriented(context));
        context.cleanup();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkScanFilterAndProjectOperator.class).run();
    }

    public static class PageRecordCursor
            implements RecordCursor
    {
        private final List<Type> types;
        private final List<Page> pages;
        private int currentPageIndex;
        private int position = -1;

        private PageRecordCursor(List<Type> types, List<Page> pages)
        {
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.pages = requireNonNull(pages, "page is null");
            checkArgument(types.size() == pages.get(currentPageIndex).getChannelCount(), "Types do not match page channels");
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            return types.get(field);
        }

        @Override
        public boolean advanceNextPosition()
        {
            position++;
            if (position >= pages.get(currentPageIndex).getPositionCount()) {
                currentPageIndex++;
                position = 0;
            }
            return currentPageIndex < pages.size();
        }

        @Override
        public boolean getBoolean(int field)
        {
            checkState(position >= 0, "Not yet advanced");
            checkState(position < pages.get(currentPageIndex).getPositionCount(), "Already finished");
            Type type = types.get(field);
            return type.getBoolean(pages.get(currentPageIndex).getBlock(field), position);
        }

        @Override
        public long getLong(int field)
        {
            checkState(position >= 0, "Not yet advanced");
            checkState(position < pages.get(currentPageIndex).getPositionCount(), "Already finished");
            Type type = types.get(field);
            return type.getLong(pages.get(currentPageIndex).getBlock(field), position);
        }

        @Override
        public double getDouble(int field)
        {
            checkState(position >= 0, "Not yet advanced");
            checkState(position < pages.get(currentPageIndex).getPositionCount(), "Already finished");
            Type type = types.get(field);
            return type.getDouble(pages.get(currentPageIndex).getBlock(field), position);
        }

        @Override
        public Slice getSlice(int field)
        {
            checkState(position >= 0, "Not yet advanced");
            checkState(position < pages.get(currentPageIndex).getPositionCount(), "Already finished");
            Type type = types.get(field);
            return type.getSlice(pages.get(currentPageIndex).getBlock(field), position);
        }

        @Override
        public Object getObject(int field)
        {
            checkState(position >= 0, "Not yet advanced");
            checkState(position < pages.get(currentPageIndex).getPositionCount(), "Already finished");
            Type type = types.get(field);
            return type.getObject(pages.get(currentPageIndex).getBlock(field), position);
        }

        @Override
        public boolean isNull(int field)
        {
            checkState(position >= 0, "Not yet advanced");
            checkState(position < pages.get(currentPageIndex).getPositionCount(), "Already finished");
            return pages.get(currentPageIndex).getBlock(field).isNull(position);
        }

        @Override
        public void close() {}
    }
}
