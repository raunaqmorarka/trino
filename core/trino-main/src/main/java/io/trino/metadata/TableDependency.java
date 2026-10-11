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
package io.trino.metadata;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableDependency;
import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A table whose rows a write depends on, see {@link ConnectorTableDependency}.
 */
public record TableDependency(TableHandle tableHandle, Optional<TupleDomain<ColumnHandle>> tablePropertiesPredicate)
{
    public TableDependency
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(tablePropertiesPredicate, "tablePropertiesPredicate is null");
    }

    public ConnectorTableDependency toConnectorTableDependency()
    {
        return new ConnectorTableDependency(tableHandle.connectorHandle(), tablePropertiesPredicate);
    }
}
