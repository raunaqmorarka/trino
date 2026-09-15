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
package io.trino.plugin.hive.metastore;

import com.linkedin.coral.common.HiveMetastoreClient;
import com.linkedin.coral.hive.metastore.api.Database;
import com.linkedin.coral.hive.metastore.api.Table;
import io.trino.plugin.hive.SchemaMappingPrefixes.SchemaPrefix;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Resolves schema names of a Hive view body against the schema mapping prefix the view is stored under.
 */
public class SchemaMappingCoralMetastoreClient
        implements HiveMetastoreClient
{
    private final HiveMetastoreClient delegate;
    private final SchemaPrefix prefix;

    public SchemaMappingCoralMetastoreClient(HiveMetastoreClient delegate, SchemaPrefix prefix)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.prefix = requireNonNull(prefix, "prefix is null");
    }

    /**
     * Lists schemas of the prefix under their unprefixed name as well, because Coral only resolves listed schemas.
     */
    @Override
    public List<String> getAllDatabases()
    {
        List<String> databases = delegate.getAllDatabases();
        return Stream.concat(
                        databases.stream(),
                        databases.stream()
                                .filter(name -> name.startsWith(prefix.value()))
                                .map(name -> name.substring(prefix.value().length())))
                .distinct()
                .collect(toImmutableList());
    }

    @Override
    public Database getDatabase(String dbName)
    {
        return delegate.getDatabase(prefix.apply(dbName));
    }

    @Override
    public List<String> getAllTables(String dbName)
    {
        return delegate.getAllTables(prefix.apply(dbName));
    }

    @Override
    public Table getTable(String dbName, String tableName)
    {
        return delegate.getTable(prefix.apply(dbName), tableName);
    }
}
