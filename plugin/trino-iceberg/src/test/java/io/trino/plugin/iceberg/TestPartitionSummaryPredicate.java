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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.apache.iceberg.GenericPartitionFieldSummary;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.IcebergUtil.getProjectedColumns;
import static io.trino.plugin.iceberg.PartitionSummaryPredicate.getPartitionSummaryPredicate;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.function.Function.identity;
import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPartitionSummaryPredicate
{
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "part", Types.StringType.get()),
            Types.NestedField.optional(2, "score", Types.DoubleType.get()),
            Types.NestedField.optional(3, "category", Types.StringType.get()),
            Types.NestedField.optional(4, "amount", Types.DecimalType.of(10, 2)),
            Types.NestedField.optional(5, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(6, "day", Types.DateType.get()),
            Types.NestedField.optional(7, "count", Types.LongType.get()));
    private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
            .withSpecId(0)
            .identity("part")
            .identity("score")
            .build();
    private static final PartitionSpec EVOLVED_SPEC = PartitionSpec.builderFor(SCHEMA)
            .withSpecId(1)
            .bucket("category", 4)
            .identity("part")
            .build();
    private static final PartitionSpec TYPED_SPEC = PartitionSpec.builderFor(SCHEMA)
            .withSpecId(2)
            .identity("amount")
            .identity("ts")
            .identity("day")
            .build();
    private static final PartitionSpec PROMOTED_SPEC = PartitionSpec.builderFor(SCHEMA)
            .withSpecId(3)
            .identity("count")
            .build();
    private static final Map<Integer, PartitionSpec> SPECS = ImmutableMap.of(0, SPEC, 1, EVOLVED_SPEC, 2, TYPED_SPEC, 3, PROMOTED_SPEC);
    private static final Map<Integer, IcebergColumnHandle> COLUMNS = getProjectedColumns(SCHEMA, TESTING_TYPE_MANAGER, ImmutableSet.of(1, 2, 4, 5, 6, 7)).stream()
            .collect(toImmutableMap(IcebergColumnHandle::getId, identity()));
    private static final IcebergColumnHandle PART = COLUMNS.get(1);

    @Test
    void testUnionOfManifestRanges()
    {
        assertThat(predicate(
                ImmutableList.of(manifest(range("a", "c", false)), manifest(range("x", "z", false))),
                TupleDomain.all(),
                1000))
                .isEqualTo(partDomain(Domain.create(ValueSet.ofRanges(varcharRange("a", "c"), varcharRange("x", "z")), false)));
    }

    @Test
    void testNulls()
    {
        assertThat(predicate(ImmutableList.of(manifest(range("a", "c", true))), TupleDomain.all(), 1000))
                .isEqualTo(partDomain(Domain.create(ValueSet.ofRanges(varcharRange("a", "c")), true)));
        assertThat(predicate(ImmutableList.of(manifest(new GenericPartitionFieldSummary(true, false, null, null))), TupleDomain.all(), 1000))
                .isEqualTo(partDomain(Domain.onlyNull(VARCHAR)));
    }

    @Test
    void testMissingBounds()
    {
        assertThat(predicate(ImmutableList.of(manifest(new GenericPartitionFieldSummary(false, false, null, null))), TupleDomain.all(), 1000))
                .isEqualTo(TupleDomain.all());
        assertThat(predicate(ImmutableList.of(new TestingManifestFile(0, 1, 0, null)), TupleDomain.all(), 1000))
                .isEqualTo(TupleDomain.all());
    }

    @Test
    void testInvertedBounds()
    {
        assertThat(predicate(ImmutableList.of(manifest(range("z", "a", false))), TupleDomain.all(), 1000))
                .isEqualTo(TupleDomain.all());
    }

    @Test
    void testManifestWithOnlyDeletedFiles()
    {
        ManifestFile deleted = new TestingManifestFile(0, 0, 0, summaries(range("x", "z", false)));
        assertThat(predicate(ImmutableList.of(manifest(range("a", "c", false)), deleted), TupleDomain.all(), 1000))
                .isEqualTo(partDomain(Domain.create(ValueSet.ofRanges(varcharRange("a", "c")), false)));
        assertThat(predicate(ImmutableList.of(deleted), TupleDomain.all(), 1000))
                .isEqualTo(TupleDomain.none());

        ManifestFile unknownCounts = new TestingManifestFile(0, null, null, summaries(range("x", "z", false)));
        assertThat(predicate(ImmutableList.of(unknownCounts), TupleDomain.all(), 1000))
                .isEqualTo(partDomain(Domain.create(ValueSet.ofRanges(varcharRange("x", "z")), false)));
    }

    @Test
    void testEnforcedPredicateNarrowsManifests()
    {
        TupleDomain<IcebergColumnHandle> enforced = partDomain(Domain.singleValue(VARCHAR, utf8Slice("b")));
        assertThat(predicate(ImmutableList.of(manifest(range("a", "c", false)), manifest(range("x", "z", false))), enforced, 1000))
                .isEqualTo(enforced);
        assertThat(predicate(ImmutableList.of(manifest(range("x", "z", false))), enforced, 1000))
                .isEqualTo(TupleDomain.none());
    }

    @Test
    void testDomainCompactionThreshold()
    {
        assertThat(predicate(
                ImmutableList.of(manifest(range("a", "c", false)), manifest(range("m", "n", false)), manifest(range("x", "z", false))),
                TupleDomain.all(),
                2))
                .isEqualTo(partDomain(Domain.create(ValueSet.ofRanges(varcharRange("a", "z")), false)));
    }

    @Test
    void testPartitionEvolution()
    {
        // The evolved spec places the identity field after a bucket field
        ManifestFile evolved = new TestingManifestFile(1, 1, 0, ImmutableList.of(summary(Types.IntegerType.get(), 0, 3), range("x", "z", false)));
        assertThat(predicate(ImmutableList.of(manifest(range("a", "c", false)), evolved), TupleDomain.all(), 1000))
                .isEqualTo(partDomain(Domain.create(ValueSet.ofRanges(varcharRange("a", "c"), varcharRange("x", "z")), false)));
    }

    @Test
    void testTypedBounds()
    {
        ManifestFile typed = new TestingManifestFile(2, 1, 0, ImmutableList.of(
                summary(Types.DecimalType.of(10, 2), new BigDecimal("10.50"), new BigDecimal("20.00")),
                summary(Types.TimestampType.withoutZone(), 1_000_000L, 2_000_000L),
                summary(Types.DateType.get(), 100, 200)));
        assertThat(predicate(ImmutableList.of(typed), TupleDomain.all(), 1000))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                        COLUMNS.get(4), Domain.create(ValueSet.ofRanges(Range.range(createDecimalType(10, 2), 1050L, true, 2000L, true)), false),
                        COLUMNS.get(5), Domain.create(ValueSet.ofRanges(Range.range(TIMESTAMP_MICROS, 1_000_000L, true, 2_000_000L, true)), false),
                        COLUMNS.get(6), Domain.create(ValueSet.ofRanges(Range.range(DATE, 100L, true, 200L, true)), false))));
    }

    @Test
    void testBoundsWrittenBeforeTypePromotion()
    {
        // Bounds written while the column was an integer decode under the promoted long type
        ManifestFile promoted = new TestingManifestFile(3, 1, 0, ImmutableList.of(summary(Types.IntegerType.get(), 1, 5)));
        assertThat(predicate(ImmutableList.of(promoted), TupleDomain.all(), 1000))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                        COLUMNS.get(7), Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 5L, true)), false))));
    }

    @Test
    void testStopsAtManifestCoveringEnforcedPredicate()
    {
        // The manifest with an unknown spec is never read once the union covers the enforced predicate
        ManifestFile unknownSpec = new TestingManifestFile(99, 1, 0, summaries(range("x", "z", false)));
        TupleDomain<IcebergColumnHandle> enforced = partDomain(Domain.singleValue(VARCHAR, utf8Slice("b")));
        assertThat(predicate(ImmutableList.of(manifest(range("a", "c", false)), unknownSpec), enforced, 1000))
                .isEqualTo(enforced);
        assertThat(predicate(ImmutableList.of(new TestingManifestFile(0, 1, 0, null), unknownSpec), TupleDomain.all(), 1000))
                .isEqualTo(TupleDomain.all());
    }

    private static TupleDomain<IcebergColumnHandle> predicate(List<ManifestFile> manifests, TupleDomain<IcebergColumnHandle> enforcedPredicate, int domainCompactionThreshold)
    {
        return getPartitionSummaryPredicate(manifests, SPECS, COLUMNS, enforcedPredicate, domainCompactionThreshold);
    }

    private static TupleDomain<IcebergColumnHandle> partDomain(Domain domain)
    {
        return TupleDomain.withColumnDomains(ImmutableMap.of(PART, domain));
    }

    private static Range varcharRange(String low, String high)
    {
        return Range.range(VARCHAR, utf8Slice(low), true, utf8Slice(high), true);
    }

    private static ManifestFile.PartitionFieldSummary range(String low, String high, boolean containsNull)
    {
        return new GenericPartitionFieldSummary(
                containsNull,
                false,
                toByteBuffer(Types.StringType.get(), low),
                toByteBuffer(Types.StringType.get(), high));
    }

    private static ManifestFile.PartitionFieldSummary summary(Type type, Object low, Object high)
    {
        return new GenericPartitionFieldSummary(false, false, toByteBuffer(type, low), toByteBuffer(type, high));
    }

    private static ManifestFile manifest(ManifestFile.PartitionFieldSummary partSummary)
    {
        return new TestingManifestFile(0, 1, 0, summaries(partSummary));
    }

    private static List<ManifestFile.PartitionFieldSummary> summaries(ManifestFile.PartitionFieldSummary partSummary)
    {
        return ImmutableList.of(partSummary, summary(Types.DoubleType.get(), 1.0, 2.0));
    }

    private record TestingManifestFile(int partitionSpecId, Integer addedFilesCount, Integer existingFilesCount, List<ManifestFile.PartitionFieldSummary> partitions)
            implements ManifestFile
    {
        @Override
        public String path()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long length()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ManifestContent content()
        {
            return ManifestContent.DATA;
        }

        @Override
        public long sequenceNumber()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long minSequenceNumber()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long snapshotId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long addedRowsCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long existingRowsCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Integer deletedFilesCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long deletedRowsCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ManifestFile copy()
        {
            throw new UnsupportedOperationException();
        }
    }
}
