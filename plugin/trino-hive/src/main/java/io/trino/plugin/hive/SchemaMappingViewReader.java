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
package io.trino.plugin.hive;

import com.linkedin.coral.common.HiveMetastoreClient;
import io.trino.metastore.Table;
import io.trino.plugin.hive.SchemaMappingPrefixes.SchemaPrefix;
import io.trino.plugin.hive.ViewReaderUtil.ViewReader;
import io.trino.plugin.hive.metastore.SchemaMappingCoralMetastoreClient;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorViewDefinition;

import java.util.Optional;

import static io.trino.plugin.hive.SchemaMappingSqlRewriter.prefixSchemaNames;
import static java.util.Objects.requireNonNull;

/**
 * Resolves the schema names of a view body against the schema mapping prefix the view is stored under.
 */
public class SchemaMappingViewReader
        implements ViewReader
{
    private final ViewReader delegate;
    private final SchemaPrefix prefix;

    /**
     * Wraps the reader of a view stored under a schema mapping prefix.
     */
    public static ViewReader wrap(ViewReader delegate, SchemaMappingPrefixes prefixes, String schemaName)
    {
        Optional<SchemaPrefix> prefix = prefixes.findPrefix(schemaName);
        if (prefix.isEmpty()) {
            return delegate;
        }
        return new SchemaMappingViewReader(delegate, prefix.get());
    }

    /**
     * Resolves the sub schemas Coral reads against the prefix, because a Hive view body names them without it.
     */
    public static HiveMetastoreClient coralClient(HiveMetastoreClient delegate, SchemaMappingPrefixes prefixes, String schemaName)
    {
        Optional<SchemaPrefix> prefix = prefixes.findPrefix(schemaName);
        if (prefix.isEmpty()) {
            return delegate;
        }
        return new SchemaMappingCoralMetastoreClient(delegate, prefix.get());
    }

    private SchemaMappingViewReader(ViewReader delegate, SchemaPrefix prefix)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.prefix = requireNonNull(prefix, "prefix is null");
    }

    @Override
    public ConnectorViewDefinition decodeViewData(Optional<String> viewData, Table table, CatalogName catalogName)
    {
        return prefixSchemaNames(delegate.decodeViewData(viewData, table, catalogName), prefix, catalogName);
    }
}
