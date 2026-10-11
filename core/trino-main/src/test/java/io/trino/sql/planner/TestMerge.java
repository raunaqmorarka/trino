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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DiscretePredicates;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.testing.PlanTester;
import org.assertj.core.groups.Tuple;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.OPTIMIZE_METADATA_QUERIES;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.spi.StandardErrorCode.MERGE_TARGET_ROW_MULTIPLE_MATCHES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class TestMerge
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction FAIL = FUNCTIONS.resolveFunction("fail", fromTypes(INTEGER, VARCHAR));
    private static final ResolvedFunction NOT = FUNCTIONS.resolveFunction("$not", fromTypes(BOOLEAN));
    private static final Type ROW_TYPE = RowType.anonymousRow(INTEGER, INTEGER, BOOLEAN, TINYINT, INTEGER);
    private static final String PARTITIONED_TABLE = "test_table_merge_partitioned";
    private static final ColumnHandle COLUMN1 = new MockConnectorColumnHandle("column1", INTEGER);
    // Table properties of the partitioned table report a single partition with column1 = 1
    private static final TupleDomain<ColumnHandle> PARTITION_PREDICATE = TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN1, Domain.singleValue(INTEGER, 1L)));

    @Override
    protected PlanTester createPlanTester()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("mock")
                .setSchema("schema")
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false");

        PlanTester planTester = PlanTester.create(sessionBuilder.build());
        planTester.installPlugin(
                new MockConnectorPlugin(MockConnectorFactory.builder()
                        .withGetTableHandle((_, schemaTableName) -> {
                            if (schemaTableName.getTableName().equals("test_table_merge_target")) {
                                return new MockConnectorTableHandle(schemaTableName);
                            }

                            if (schemaTableName.getTableName().equals("test_table_merge_source")) {
                                return new MockConnectorTableHandle(schemaTableName);
                            }

                            if (schemaTableName.getTableName().equals(PARTITIONED_TABLE)) {
                                return new MockConnectorTableHandle(schemaTableName);
                            }

                            return null;
                        })
                        .withGetColumns(_ -> ImmutableList.of(
                                new ColumnMetadata("column1", INTEGER),
                                new ColumnMetadata("column2", INTEGER)))
                        .withApplyFilter((_, tableHandle, constraint) -> {
                            MockConnectorTableHandle handle = (MockConnectorTableHandle) tableHandle;
                            if (!isPartitionedTable(handle)) {
                                return Optional.empty();
                            }
                            MockConnectorTableHandle newHandle = new MockConnectorTableHandle(handle.getTableName(), handle.getConstraint().intersect(constraint.getSummary()), handle.getColumns());
                            return Optional.of(new ConstraintApplicationResult<>(newHandle, TupleDomain.all(), constraint.getExpression(), false));
                        })
                        .withGetTableProperties((_, tableHandle) -> {
                            MockConnectorTableHandle handle = (MockConnectorTableHandle) tableHandle;
                            if (!isPartitionedTable(handle)) {
                                return new ConnectorTableProperties();
                            }
                            return new ConnectorTableProperties(
                                    PARTITION_PREDICATE.intersect(handle.getConstraint()),
                                    Optional.empty(),
                                    Optional.of(new DiscretePredicates(ImmutableList.of(COLUMN1), ImmutableList.of(PARTITION_PREDICATE))),
                                    ImmutableList.of());
                        })
                        .build()));
        planTester.createCatalog("mock", "mock", ImmutableMap.of());
        return planTester;
    }

    @Test
    public void testTableDependenciesOfScannedTables()
    {
        // The join derives predicates from table properties, so the target read also depends on the partition predicate
        assertThat(tableDependencies(
                "MERGE INTO " + PARTITIONED_TABLE + " t USING test_table_merge_source s ON t.column1 = s.column1 " +
                        "WHEN MATCHED THEN UPDATE SET column2 = s.column2"))
                .containsExactly(
                        tuple(PARTITIONED_TABLE, Optional.empty()),
                        tuple("test_table_merge_source", Optional.empty()),
                        tuple(PARTITIONED_TABLE, Optional.of(PARTITION_PREDICATE)));
    }

    @Test
    public void testTableDependenciesOfScanRemovedByTableProperties()
    {
        // No partition can match column1 = 2, so the subquery scan is removed and the write depends on that staying true
        assertThat(tableDependencies("DELETE FROM " + PARTITIONED_TABLE + " WHERE NOT EXISTS (SELECT 1 FROM " + PARTITIONED_TABLE + " WHERE column1 = 2)"))
                .containsExactly(
                        tuple(PARTITIONED_TABLE, Optional.empty()),
                        tuple(PARTITIONED_TABLE, Optional.of(TupleDomain.none())),
                        tuple(PARTITIONED_TABLE, Optional.of(PARTITION_PREDICATE)));
    }

    @Test
    public void testTableDependenciesOfMetadataOptimizedScan()
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(OPTIMIZE_METADATA_QUERIES, "true")
                .build();
        assertThat(tableDependencies(
                session,
                "MERGE INTO test_table_merge_target t USING (SELECT max(column1) AS column1 FROM " + PARTITIONED_TABLE + ") s ON t.column1 = s.column1 " +
                        "WHEN MATCHED THEN UPDATE SET column2 = s.column1"))
                .contains(tuple(PARTITIONED_TABLE, Optional.empty()))
                .doesNotContain(tuple(PARTITIONED_TABLE, Optional.of(PARTITION_PREDICATE)));
    }

    @Test
    public void testMergeWithSimpleSelect()
    {
        // one join
        assertPlan(
                "MERGE INTO test_table_merge_target a " +
                        "USING test_table_merge_source b " +
                        "ON a.column1 = b.column1 " +
                        "WHEN MATCHED " +
                        "THEN UPDATE SET column2 = b.column2 " +
                        "WHEN NOT MATCHED " +
                        "THEN INSERT (column1 ,column2) VALUES (b.column1, b.column2)",
                anyTree(
                        filter(
                                new Case(
                                        ImmutableList.of(new WhenClause(new Call(NOT, ImmutableList.of(new Reference(BOOLEAN, "is_distinct"))), new Cast(new Call(FAIL, ImmutableList.of(new Constant(INTEGER, (long) MERGE_TARGET_ROW_MULTIPLE_MATCHES.toErrorCode().getCode()), new Constant(VARCHAR, utf8Slice("One MERGE target table row matched more than one source row")))), BOOLEAN))),
                                        TRUE),
                                markDistinct("is_distinct", ImmutableList.of("unique_id", "case_number"),
                                        anyTree(
                                                project(ImmutableMap.of(
                                                                "unique_id", expression(new Coalesce(ImmutableList.of(new Reference(BIGINT, "target_unique_id"), new Reference(BIGINT, "source_unique_id")))),
                                                                "field", expression(new Reference(BIGINT, "field")),
                                                                "merge_row", expression(new Reference(ROW_TYPE, "merge_row")),
                                                                "case_number", expression(new FieldReference(new Reference(ROW_TYPE, "merge_row"), 4))),
                                                        project(ImmutableMap.of(
                                                                        "field", expression(new Reference(BIGINT, "field")),
                                                                        "merge_row", expression(new Case(
                                                                                ImmutableList.of(
                                                                                        new WhenClause(new Reference(BOOLEAN, "present"), new Row(ImmutableList.of(new Reference(INTEGER, "column1"), new Reference(INTEGER, "column2_1"), new Call(NOT, ImmutableList.of(new IsNull(new Reference(BOOLEAN, "present")))), new Constant(TINYINT, 3L), new Constant(INTEGER, 0L)), ROW_TYPE)),
                                                                                        new WhenClause(new IsNull(new Reference(BOOLEAN, "present")), new Row(ImmutableList.of(new Reference(INTEGER, "column1_0"), new Reference(INTEGER, "column2_1"), new Call(NOT, ImmutableList.of(new IsNull(new Reference(BOOLEAN, "present")))), new Constant(TINYINT, 1L), new Constant(INTEGER, 1L)), ROW_TYPE))),
                                                                                new Constant(ROW_TYPE, null))),
                                                                        "target_unique_id", expression(new Reference(BIGINT, "target_unique_id")),
                                                                        "source_unique_id", expression(new Reference(BIGINT, "source_unique_id"))),
                                                                join(RIGHT, builder -> builder
                                                                        .equiCriteria("column1", "column1_0")
                                                                        .left(
                                                                                project(ImmutableMap.of(
                                                                                                "column1", expression(new Reference(INTEGER, "column1")),
                                                                                                "field", expression(new Reference(BIGINT, "field")),
                                                                                                "target_unique_id", expression(new Reference(BIGINT, "target_unique_id")),
                                                                                                "present", expression(new Constant(BOOLEAN, true))),
                                                                                        assignUniqueId("target_unique_id",
                                                                                                tableScan(
                                                                                                        tableHandle -> ((MockConnectorTableHandle) tableHandle).getTableName().getTableName().equals("test_table_merge_target"),
                                                                                                        TupleDomain.all(),
                                                                                                        ImmutableMap.of(
                                                                                                                "column1", columnHandle -> ((MockConnectorColumnHandle) columnHandle).name().equals("column1"),
                                                                                                                "field", columnHandle -> ((MockConnectorColumnHandle) columnHandle).name().equals("merge_row_id"))))))
                                                                        .right(
                                                                                anyTree(
                                                                                        assignUniqueId(
                                                                                                "source_unique_id",
                                                                                                tableScan("test_table_merge_source", ImmutableMap.of("column1_0", "column1", "column2_1", "column2")))))))))))));
    }

    private static boolean isPartitionedTable(MockConnectorTableHandle handle)
    {
        return handle.getTableName().equals(new SchemaTableName("schema", PARTITIONED_TABLE));
    }

    private List<Tuple> tableDependencies(@Language("SQL") String sql)
    {
        return tableDependencies(getPlanTester().getDefaultSession(), sql);
    }

    // Returns the (table name, table properties predicate) pairs the merge writer depends on
    private List<Tuple> tableDependencies(Session session, @Language("SQL") String sql)
    {
        PlanTester planTester = getPlanTester();
        Plan plan = planTester.inTransaction(session, transactionSession ->
                planTester.createPlan(transactionSession, sql, planTester.getPlanOptimizers(false), OPTIMIZED_AND_VALIDATED, NOOP, createPlanOptimizersStatsCollector()));
        MergeWriterNode mergeWriter = (MergeWriterNode) PlanNodeSearcher.searchFrom(plan.getRoot())
                .where(MergeWriterNode.class::isInstance)
                .findOnlyElement();
        return mergeWriter.getTarget().getTableDependencies().stream()
                .map(dependency -> tuple(
                        ((MockConnectorTableHandle) dependency.tableHandle().connectorHandle()).getTableName().getTableName(),
                        dependency.tablePropertiesPredicate()))
                .collect(toImmutableList());
    }
}
