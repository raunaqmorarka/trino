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
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.TableInfo;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergSessionProperties;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.plugin.iceberg.encryption.IcebergEncryptionConfig;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.metastore.TableInfo.ExtendedRelationType.TABLE;
import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.ENCRYPTION_MANAGER_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestSchemaMappingTrinoCatalog
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new IcebergSessionProperties(
                    new IcebergConfig(),
                    new IcebergEncryptionConfig(),
                    new OrcReaderConfig(),
                    new OrcWriterConfig(),
                    new ParquetReaderConfig(),
                    new ParquetWriterConfig())
                    .getSessionProperties())
            .build();

    private static final TrinoPrincipal OWNER = new TrinoPrincipal(PrincipalType.USER, SESSION.getUser());
    private static final String PRODUCTION_PREFIX = "egdp_prod_";
    private static final String ANALYTICS_PREFIX = "egdp_analytics_";

    private Path tempDirectory;
    private TrinoCatalog local;
    private TrinoCatalog production;
    private TrinoCatalog analytics;
    private TrinoCatalog catalog;

    @BeforeAll
    public void setUp()
            throws IOException
    {
        tempDirectory = Files.createTempDirectory("test_schema_mapping_catalog");
        local = createCatalog("local");
        production = createCatalog("production");
        analytics = createCatalog("analytics");
        catalog = new SchemaMappingTrinoCatalog(
                new CatalogName("iceberg"),
                local,
                ImmutableMap.of(PRODUCTION_PREFIX, production, ANALYTICS_PREFIX, analytics));

        local.createNamespace(SESSION, "metrics_platform", ImmutableMap.of(), OWNER);
        production.createNamespace(SESSION, "metrics_platform", ImmutableMap.of(), OWNER);
        production.createNamespace(SESSION, "supply", ImmutableMap.of(), OWNER);
        analytics.createNamespace(SESSION, "metrics_platform", ImmutableMap.of(), OWNER);

        createTable(local, "metrics_platform", "local_bookings");
        createTable(production, "supply", "lodging");
        createTable(analytics, "metrics_platform", "sessions");
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        deleteRecursively(tempDirectory, ALLOW_INSECURE);
    }

    @Test
    public void testAllNamespacesAreListedUnderTheirPrefix()
    {
        assertThat(catalog.listNamespaces(SESSION)).containsExactlyInAnyOrder(
                "metrics_platform",
                "egdp_prod_metrics_platform",
                "egdp_prod_supply",
                "egdp_analytics_metrics_platform");
        assertThat(catalog.namespaceExists(SESSION, "egdp_prod_supply")).isTrue();
        assertThat(catalog.namespaceExists(SESSION, "supply")).isFalse();
    }

    @Test
    public void testTablesAreListedUnderTheirPrefixedNamespace()
    {
        assertThat(catalog.listTables(SESSION, Optional.of("egdp_prod_supply")))
                .containsExactly(new TableInfo(new SchemaTableName("egdp_prod_supply", "lodging"), TABLE));
        assertThat(catalog.listTables(SESSION, Optional.of("metrics_platform")))
                .containsExactly(new TableInfo(new SchemaTableName("metrics_platform", "local_bookings"), TABLE));
        assertThat(catalog.listTables(SESSION, Optional.empty()))
                .contains(
                        new TableInfo(new SchemaTableName("metrics_platform", "local_bookings"), TABLE),
                        new TableInfo(new SchemaTableName("egdp_prod_supply", "lodging"), TABLE),
                        new TableInfo(new SchemaTableName("egdp_analytics_metrics_platform", "sessions"), TABLE));
    }

    @Test
    public void testTableIsLoadedThroughItsPrefix()
    {
        assertThat(catalog.loadTable(SESSION, new SchemaTableName("egdp_prod_supply", "lodging")).schema().columns())
                .hasSize(1);
        assertThatThrownBy(() -> catalog.loadTable(SESSION, new SchemaTableName("supply", "lodging")))
                .hasMessageContaining("supply.lodging");
    }

    @Test
    public void testCreateAndDropNamespaceUseTheResolvedTarget()
    {
        catalog.createNamespace(SESSION, "egdp_analytics_created", ImmutableMap.of(), OWNER);
        assertThat(analytics.namespaceExists(SESSION, "created")).isTrue();
        assertThat(catalog.namespaceExists(SESSION, "egdp_analytics_created")).isTrue();

        catalog.dropNamespace(SESSION, "egdp_analytics_created");
        assertThat(analytics.namespaceExists(SESSION, "created")).isFalse();
    }

    @Test
    public void testCreateTableUsesTheResolvedTarget()
    {
        createTable(catalog, "egdp_prod_supply", "created");
        assertThat(production.listTables(SESSION, Optional.of("supply")).stream().map(TableInfo::tableName))
                .contains(new SchemaTableName("supply", "created"));

        catalog.dropTable(SESSION, new SchemaTableName("egdp_prod_supply", "created"));
        assertThat(production.listTables(SESSION, Optional.of("supply")).stream().map(TableInfo::tableName))
                .doesNotContain(new SchemaTableName("supply", "created"));
    }

    @Test
    public void testViewBodyIsResolvedAgainstThePrefix()
    {
        SchemaTableName viewName = new SchemaTableName("egdp_prod_supply", "lodging_view");
        catalog.createView(SESSION, viewName, viewDefinition("SELECT id FROM supply.lodging"), ImmutableMap.of(), false);
        try {
            ConnectorViewDefinition definition = catalog.getView(SESSION, viewName).orElseThrow();
            assertThat(definition.getOriginalSql()).isEqualTo("SELECT id FROM \"egdp_prod_supply\".lodging");
            assertThat(definition.getCatalog()).contains("iceberg");
            assertThat(definition.getSchema()).contains("egdp_prod_supply");

            Map<SchemaTableName, ConnectorViewDefinition> views = catalog.getViews(SESSION, Optional.of("egdp_prod_supply"));
            assertThat(views).containsOnlyKeys(viewName);
            assertThat(views.get(viewName).getOriginalSql()).isEqualTo("SELECT id FROM \"egdp_prod_supply\".lodging");
        }
        finally {
            catalog.dropView(SESSION, viewName);
        }
    }

    @Test
    public void testViewBodyOfUnmappedNamespaceIsUnchanged()
    {
        SchemaTableName viewName = new SchemaTableName("metrics_platform", "local_view");
        catalog.createView(SESSION, viewName, viewDefinition("SELECT id FROM supply.lodging"), ImmutableMap.of(), false);
        try {
            ConnectorViewDefinition definition = catalog.getView(SESSION, viewName).orElseThrow();
            assertThat(definition.getOriginalSql()).isEqualTo("SELECT id FROM supply.lodging");
        }
        finally {
            catalog.dropView(SESSION, viewName);
        }
    }

    @Test
    public void testViewsAreListedUnderTheirPrefixedNamespace()
    {
        SchemaTableName viewName = new SchemaTableName("egdp_prod_supply", "listed_view");
        catalog.createView(SESSION, viewName, viewDefinition("SELECT id FROM supply.lodging"), ImmutableMap.of(), false);
        try {
            assertThat(catalog.listViews(SESSION, Optional.of("egdp_prod_supply"))).containsExactly(viewName);
            assertThat(catalog.listViews(SESSION, Optional.empty())).contains(viewName);
            assertThat(catalog.listViews(SESSION, Optional.of("supply"))).isEmpty();
        }
        finally {
            catalog.dropView(SESSION, viewName);
        }
    }

    @Test
    public void testLongestPrefixWins()
            throws IOException
    {
        TrinoCatalog shortPrefix = createCatalog("short");
        TrinoCatalog longPrefix = createCatalog("long");
        shortPrefix.createNamespace(SESSION, "prod_marketing", ImmutableMap.of(), OWNER);
        longPrefix.createNamespace(SESSION, "marketing", ImmutableMap.of(), OWNER);
        createTable(shortPrefix, "prod_marketing", "short_table");
        createTable(longPrefix, "marketing", "long_table");

        TrinoCatalog overlapping = new SchemaMappingTrinoCatalog(
                new CatalogName("iceberg"),
                local,
                ImmutableMap.of("egdp_", shortPrefix, "egdp_prod_", longPrefix));
        assertThat(overlapping.listTables(SESSION, Optional.of("egdp_prod_marketing")).stream().map(info -> info.tableName().getTableName()))
                .containsExactly("long_table");
    }

    @Test
    public void testRenameAcrossPrefixesIsRejected()
    {
        assertThatThrownBy(() -> catalog.renameTable(
                SESSION,
                new SchemaTableName("egdp_prod_supply", "lodging"),
                new SchemaTableName("egdp_analytics_metrics_platform", "lodging")))
                .hasMessage("Renaming across schema mapping rules is not supported");
    }

    @Test
    public void testRenameBetweenNamespacesOfOnePrefixUsesTheResolvedNames()
    {
        createTable(catalog, "egdp_prod_supply", "renamed_source");
        catalog.renameTable(
                SESSION,
                new SchemaTableName("egdp_prod_supply", "renamed_source"),
                new SchemaTableName("egdp_prod_metrics_platform", "renamed_target"));
        try {
            assertThat(production.listTables(SESSION, Optional.of("metrics_platform")).stream().map(TableInfo::tableName))
                    .contains(new SchemaTableName("metrics_platform", "renamed_target"));
            assertThat(production.listTables(SESSION, Optional.of("supply")).stream().map(TableInfo::tableName))
                    .doesNotContain(new SchemaTableName("supply", "renamed_source"));
        }
        finally {
            catalog.dropTable(SESSION, new SchemaTableName("egdp_prod_metrics_platform", "renamed_target"));
        }
    }

    private static ConnectorViewDefinition viewDefinition(String sql)
    {
        return new ConnectorViewDefinition(
                sql,
                Optional.of("iceberg"),
                Optional.of("supply"),
                ImmutableList.of(new ViewColumn("id", BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of(SESSION.getUser()),
                false,
                ImmutableList.of());
    }

    private static void createTable(TrinoCatalog catalog, String namespace, String tableName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(namespace, tableName);
        catalog.newCreateTableTransaction(
                        SESSION,
                        schemaTableName,
                        new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())),
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        Optional.of(catalog.defaultTableLocation(SESSION, schemaTableName)),
                        ImmutableMap.of())
                .commitTransaction();
    }

    private TrinoCatalog createCatalog(String name)
            throws IOException
    {
        Path directory = Files.createDirectory(tempDirectory.resolve(name));
        TrinoFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(directory);
        CachingHiveMetastore metastore = createPerTransactionCache(createTestingFileHiveMetastore(fileSystemFactory, Location.of("local:///")), 1000);
        return new TrinoHiveCatalog(
                new CatalogName("iceberg"),
                metastore,
                new TrinoViewHiveMetastore(metastore, false, "trino-version", "test"),
                fileSystemFactory,
                FILE_IO_FACTORY,
                TESTING_TYPE_MANAGER,
                new FileMetastoreTableOperationsProvider(fileSystemFactory, FILE_IO_FACTORY, ENCRYPTION_MANAGER_FACTORY),
                false,
                false,
                false,
                new IcebergConfig().isHideMaterializedViewStorageTable(),
                directExecutor(),
                newDirectExecutorService());
    }
}
