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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.TypeID;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.immutableEnumSet;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static org.apache.iceberg.types.Conversions.fromByteBuffer;

/**
 * Derives a predicate on identity partition columns from the partition summaries in a snapshot's manifest list.
 */
final class PartitionSummaryPredicate
{
    // Types whose Iceberg ordering matches the Trino ordering
    private static final Set<TypeID> SUPPORTED_TYPES = immutableEnumSet(
            TypeID.BOOLEAN,
            TypeID.INTEGER,
            TypeID.LONG,
            TypeID.DECIMAL,
            TypeID.DATE,
            TypeID.TIME,
            TypeID.TIMESTAMP,
            TypeID.TIMESTAMP_NANO,
            TypeID.STRING,
            TypeID.BINARY);

    private PartitionSummaryPredicate() {}

    /**
     * Returns a predicate that holds for every row in the data manifests that overlap the enforced predicate.
     */
    public static TupleDomain<IcebergColumnHandle> getPartitionSummaryPredicate(
            List<ManifestFile> dataManifests,
            Map<Integer, PartitionSpec> specsById,
            Map<Integer, IcebergColumnHandle> identityPartitionColumnsById,
            TupleDomain<IcebergColumnHandle> enforcedPredicate,
            int domainCompactionThreshold)
    {
        TupleDomain<IcebergColumnHandle> predicate = TupleDomain.none();
        for (ManifestFile manifest : dataManifests) {
            // Skip manifests whose entries are all deleted
            if (!manifest.hasAddedFiles() && !manifest.hasExistingFiles()) {
                continue;
            }
            TupleDomain<IcebergColumnHandle> manifestPredicate = getManifestPredicate(manifest, specsById.get(manifest.partitionSpecId()), identityPartitionColumnsById)
                    .intersect(enforcedPredicate);
            predicate = TupleDomain.columnWiseUnion(predicate, manifestPredicate);
            // The union only grows, so once it covers the enforced predicate the remaining manifests add nothing
            if (predicate.contains(enforcedPredicate)) {
                return enforcedPredicate;
            }
        }
        return predicate.simplify(domainCompactionThreshold);
    }

    private static TupleDomain<IcebergColumnHandle> getManifestPredicate(ManifestFile manifest, PartitionSpec spec, Map<Integer, IcebergColumnHandle> identityPartitionColumnsById)
    {
        List<ManifestFile.PartitionFieldSummary> summaries = manifest.partitions();
        if (summaries == null) {
            return TupleDomain.all();
        }
        ImmutableMap.Builder<IcebergColumnHandle, Domain> domains = ImmutableMap.builder();
        List<PartitionField> fields = spec.fields();
        for (int index = 0; index < fields.size(); index++) {
            PartitionField field = fields.get(index);
            if (!field.transform().isIdentity()) {
                continue;
            }
            IcebergColumnHandle column = identityPartitionColumnsById.get(field.sourceId());
            if (column != null) {
                Type icebergType = spec.partitionType().fields().get(index).type();
                domains.put(column, getDomain(column, icebergType, summaries.get(index)));
            }
        }
        return TupleDomain.withColumnDomains(domains.buildOrThrow());
    }

    private static Domain getDomain(IcebergColumnHandle column, Type icebergType, ManifestFile.PartitionFieldSummary summary)
    {
        io.trino.spi.type.Type trinoType = column.getType();
        if (!SUPPORTED_TYPES.contains(icebergType.typeId())) {
            return Domain.all(trinoType);
        }
        if (summary.lowerBound() == null && summary.upperBound() == null && summary.containsNull()) {
            // Bounds are absent when every value is null
            return Domain.onlyNull(trinoType);
        }
        if (summary.lowerBound() == null || summary.upperBound() == null) {
            return Domain.all(trinoType);
        }
        Range lowerBound = Range.greaterThanOrEqual(trinoType, convertIcebergValueToTrino(icebergType, fromByteBuffer(icebergType, summary.lowerBound())));
        Range upperBound = Range.lessThanOrEqual(trinoType, convertIcebergValueToTrino(icebergType, fromByteBuffer(icebergType, summary.upperBound())));
        // Bounds written in an ordering other than Trino's can be inverted and then say nothing about the values
        Optional<Range> range = lowerBound.intersect(upperBound);
        if (range.isEmpty()) {
            return Domain.all(trinoType);
        }
        return Domain.create(ValueSet.ofRanges(range.get()), summary.containsNull());
    }
}
