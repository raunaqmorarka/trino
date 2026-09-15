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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.hive.metastore.glue.SchemaMappingDelegates;
import io.trino.plugin.hive.metastore.glue.SchemaMappingDelegates.SchemaMappingRule;
import io.trino.plugin.hive.security.UsingSystemSecurity;
import io.trino.plugin.iceberg.ForIcebergMetadata;
import io.trino.plugin.iceberg.ForIcebergSplitManager;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.encryption.EncryptionManagerFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.NodeVersion;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Serves the namespaces of the default Glue account together with the prefixed namespaces of the mapped accounts.
 */
public class SchemaMappingTrinoCatalogFactory
        implements TrinoCatalogFactory
{
    private final CatalogName catalogName;
    private final GlueMetastoreStats stats;
    private final TrinoGlueCatalogFactory defaultFactory;
    private final Map<String, TrinoCatalogFactory> factoriesByPrefix;

    @Inject
    public SchemaMappingTrinoCatalogFactory(
            CatalogName catalogName,
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            NodeVersion nodeVersion,
            GlueHiveMetastoreConfig glueConfig,
            IcebergConfig icebergConfig,
            IcebergGlueCatalogConfig catalogConfig,
            @UsingSystemSecurity boolean usingSystemSecurity,
            GlueMetastoreStats stats,
            GlueClient glueClient,
            @ForIcebergMetadata ExecutorService metadataExecutorService,
            @ForIcebergSplitManager ExecutorService icebergScanExecutor,
            EncryptionManagerFactory encryptionManagerFactory)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.defaultFactory = new TrinoGlueCatalogFactory(
                catalogName,
                fileSystemFactory,
                fileIoFactory,
                typeManager,
                tableOperationsProvider,
                nodeVersion,
                glueConfig,
                icebergConfig,
                catalogConfig,
                usingSystemSecurity,
                stats,
                glueClient,
                metadataExecutorService,
                icebergScanExecutor);

        Function<SchemaMappingRule, TrinoCatalogFactory> createFactory = rule -> {
            if (rule.catalogId().isEmpty()) {
                return defaultFactory;
            }
            GlueClient ruleGlueClient = SchemaMappingDelegates.createGlueClient(glueConfig, rule.catalogId());
            GlueMetastoreStats ruleStats = new GlueMetastoreStats();
            return new TrinoGlueCatalogFactory(
                    catalogName,
                    fileSystemFactory,
                    fileIoFactory,
                    typeManager,
                    new GlueIcebergTableOperationsProvider(
                            fileSystemFactory,
                            fileIoFactory,
                            typeManager,
                            catalogConfig,
                            ruleStats,
                            ruleGlueClient,
                            encryptionManagerFactory),
                    nodeVersion,
                    glueConfig,
                    icebergConfig,
                    catalogConfig,
                    usingSystemSecurity,
                    ruleStats,
                    ruleGlueClient,
                    metadataExecutorService,
                    icebergScanExecutor);
        };
        this.factoriesByPrefix = glueConfig.getSchemaMappingRules()
                .map(SchemaMappingDelegates::parseRules)
                .orElseGet(ImmutableList::of)
                .stream()
                .collect(toImmutableMap(SchemaMappingRule::prefix, createFactory));
    }

    @Managed
    @Flatten
    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        TrinoCatalog defaultCatalog = defaultFactory.create(identity);
        if (factoriesByPrefix.isEmpty()) {
            return defaultCatalog;
        }
        Map<String, TrinoCatalog> delegatesByPrefix = factoriesByPrefix.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> {
                            if (entry.getValue() == defaultFactory) {
                                return defaultCatalog;
                            }
                            return entry.getValue().create(identity);
                        }));
        return new SchemaMappingTrinoCatalog(catalogName, defaultCatalog, delegatesByPrefix);
    }
}
