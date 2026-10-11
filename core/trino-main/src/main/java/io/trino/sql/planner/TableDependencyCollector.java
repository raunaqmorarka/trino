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
import io.trino.metadata.TableDependency;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Records table reads about which the optimizer assumes more than the scan handle conveys,
 * either because it replaced the scan with table metadata or because it derived a predicate
 * from table properties while the scan survives.
 */
public class TableDependencyCollector
{
    private final Set<TableDependency> dependencies = new LinkedHashSet<>();

    /**
     * Records that the plan assumes every row matching the handle satisfies the given predicate from table properties.
     */
    public void addTablePropertiesPredicate(TableHandle tableHandle, TupleDomain<ColumnHandle> tablePropertiesPredicate)
    {
        dependencies.add(new TableDependency(tableHandle, Optional.of(tablePropertiesPredicate)));
    }

    /**
     * Records that the plan resolved the rows matching the handle from table metadata.
     */
    public void addMetadataRead(TableHandle tableHandle)
    {
        dependencies.add(new TableDependency(tableHandle, Optional.empty()));
    }

    public List<TableDependency> getDependencies()
    {
        return ImmutableList.copyOf(dependencies);
    }
}
