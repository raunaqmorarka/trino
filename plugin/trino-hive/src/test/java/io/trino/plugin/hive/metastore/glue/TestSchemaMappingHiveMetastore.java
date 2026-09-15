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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.NodeVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestSchemaMappingHiveMetastore
{
    private static final String PRODUCTION_PREFIX = "egdp_prod_";
    private static final String ANALYTICS_PREFIX = "egdp_analytics_";

    private Path tempDirectory;
    private HiveMetastore local;
    private HiveMetastore production;
    private HiveMetastore analytics;
    private HiveMetastore metastore;

    @BeforeAll
    public void setUp()
            throws IOException
    {
        tempDirectory = Files.createTempDirectory("test_schema_mapping_metastore");
        local = createMetastore("local");
        production = createMetastore("production");
        analytics = createMetastore("analytics");
        metastore = new SchemaMappingHiveMetastore(local, ImmutableMap.of(PRODUCTION_PREFIX, production, ANALYTICS_PREFIX, analytics));

        createDatabase(local, "default");
        createDatabase(local, "metrics_platform");
        createDatabase(production, "metrics_platform");
        createDatabase(production, "supply");
        createDatabase(analytics, "metrics_platform");

        createTable(local, "metrics_platform", "local_bookings");
        createTable(production, "metrics_platform", "bookings");
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
    public void testAllDatabasesAreListedUnderTheirPrefix()
    {
        assertThat(metastore.getAllDatabases()).containsExactlyInAnyOrder(
                "default",
                "metrics_platform",
                "egdp_prod_metrics_platform",
                "egdp_prod_supply",
                "egdp_analytics_metrics_platform");
    }

    @Test
    public void testDatabaseIsReturnedUnderItsPrefixedName()
    {
        assertThat(metastore.getDatabase("egdp_prod_supply"))
                .map(Database::getDatabaseName)
                .contains("egdp_prod_supply");
        assertThat(metastore.getDatabase("supply")).isEmpty();
    }

    @Test
    public void testSameNameInDifferentTargetsResolvesSeparately()
    {
        assertThat(tableNames("metrics_platform")).containsExactly("local_bookings");
        assertThat(tableNames("egdp_prod_metrics_platform")).containsExactly("bookings");
        assertThat(tableNames("egdp_analytics_metrics_platform")).containsExactly("sessions");
    }

    @Test
    public void testTableIsReturnedUnderItsPrefixedDatabaseName()
    {
        Optional<Table> table = metastore.getTable("egdp_prod_supply", "lodging");
        assertThat(table).map(Table::getDatabaseName).contains("egdp_prod_supply");
        assertThat(table).map(Table::getTableName).contains("lodging");
        assertThat(metastore.getTable("supply", "lodging")).isEmpty();
    }

    @Test
    public void testUnprefixedNameReadsTheDefaultTarget()
    {
        assertThat(metastore.getTable("metrics_platform", "local_bookings")).isPresent();
        assertThat(metastore.getTable("metrics_platform", "bookings")).isEmpty();
    }

    @Test
    public void testLongestPrefixWins()
            throws IOException
    {
        HiveMetastore shortPrefix = createMetastore("short");
        HiveMetastore longPrefix = createMetastore("long");
        createDatabase(shortPrefix, "prod_marketing");
        createDatabase(longPrefix, "marketing");
        createTable(shortPrefix, "prod_marketing", "short_table");
        createTable(longPrefix, "marketing", "long_table");

        HiveMetastore overlapping = new SchemaMappingHiveMetastore(local, ImmutableMap.of("egdp_", shortPrefix, "egdp_prod_", longPrefix));
        assertThat(overlapping.getTables("egdp_prod_marketing").stream().map(tableInfo -> tableInfo.tableName().getTableName()))
                .containsExactly("long_table");
    }

    @Test
    public void testCreateAndDropTableUseTheResolvedTarget()
    {
        metastore.createTable(table("egdp_prod_supply", "created"), NO_PRIVILEGES);
        assertThat(production.getTable("supply", "created")).isPresent();
        assertThat(metastore.getTable("egdp_prod_supply", "created")).isPresent();

        metastore.dropTable("egdp_prod_supply", "created", false);
        assertThat(production.getTable("supply", "created")).isEmpty();
    }

    @Test
    public void testCreateAndDropDatabaseUseTheResolvedTarget()
    {
        metastore.createDatabase(database("egdp_analytics_created"));
        assertThat(analytics.getDatabase("created")).isPresent();
        assertThat(metastore.getDatabase("egdp_analytics_created")).isPresent();

        metastore.dropDatabase("egdp_analytics_created", false);
        assertThat(analytics.getDatabase("created")).isEmpty();
    }

    @Test
    public void testRenameAcrossTargetsIsRejected()
    {
        assertThatThrownBy(() -> metastore.renameDatabase("egdp_prod_supply", "egdp_analytics_supply"))
                .hasMessage("Rename across schema mapping targets is not supported");
        assertThatThrownBy(() -> metastore.renameTable("egdp_prod_supply", "lodging", "metrics_platform", "lodging"))
                .hasMessage("Rename across schema mapping targets is not supported");
    }

    private HiveMetastore createMetastore(String name)
            throws IOException
    {
        Path directory = Files.createDirectory(tempDirectory.resolve(name));
        return new FileHiveMetastore(
                new NodeVersion("testversion"),
                new LocalFileSystemFactory(directory),
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(Location.of("local:///").toString())
                        .setDisableLocationChecks(true)
                        .setMetastoreUser("test"));
    }

    private ImmutableList<String> tableNames(String databaseName)
    {
        return metastore.getTables(databaseName).stream()
                .map(TableInfo::tableName)
                .map(SchemaTableName::getTableName)
                .collect(toImmutableList());
    }

    private static void createDatabase(HiveMetastore metastore, String databaseName)
    {
        metastore.createDatabase(database(databaseName));
    }

    private static Database database(String databaseName)
    {
        return Database.builder()
                .setDatabaseName(databaseName)
                .setOwnerName(Optional.of("test"))
                .setOwnerType(Optional.of(PrincipalType.USER))
                .build();
    }

    private static void createTable(HiveMetastore metastore, String databaseName, String tableName)
    {
        metastore.createTable(table(databaseName, tableName), NO_PRIVILEGES);
    }

    private static Table table(String databaseName, String tableName)
    {
        return Table.builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.of("test"))
                .setDataColumns(ImmutableList.of(new Column("id", HIVE_LONG, Optional.empty(), ImmutableMap.of())))
                .withStorage(storage -> storage
                        .setStorageFormat(TEXTFILE.toStorageFormat())
                        .setLocation(Optional.of("local:///" + databaseName + "/" + tableName)))
                .build();
    }
}
