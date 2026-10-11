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
package io.trino.spi.connector;

import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A table whose rows a write depends on.
 * <p>
 * When {@code tablePropertiesPredicate} is empty, the plan reads the rows matching {@code tableHandle},
 * or resolved them from table metadata, so a concurrent change to any of those rows is a conflict.
 * When it is present, the plan assumed every row matching {@code tableHandle} satisfies the predicate
 * returned by {@link ConnectorMetadata#getTableProperties}, so only a concurrent change to a row
 * matching the handle but not the predicate is a conflict.
 * <p>
 * The handle carries the table version the plan read, so a connector compares it against the
 * version the write is based on and ignores reads of other versions.
 */
public record ConnectorTableDependency(ConnectorTableHandle tableHandle, Optional<TupleDomain<ColumnHandle>> tablePropertiesPredicate)
{
    public ConnectorTableDependency
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(tablePropertiesPredicate, "tablePropertiesPredicate is null");
    }
}
