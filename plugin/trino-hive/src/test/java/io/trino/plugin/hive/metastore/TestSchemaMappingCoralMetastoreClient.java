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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.coral.common.HiveMetastoreClient;
import com.linkedin.coral.hive.metastore.api.Database;
import com.linkedin.coral.hive.metastore.api.Table;
import io.trino.plugin.hive.SchemaMappingPrefixes;
import io.trino.plugin.hive.SchemaMappingPrefixes.SchemaPrefix;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSchemaMappingCoralMetastoreClient
{
    private static final SchemaPrefix PREFIX = new SchemaPrefix("egdp_prod_", new SchemaMappingPrefixes(ImmutableSet.of("egdp_prod_", "egdp_test_")));

    @Test
    public void testSchemasAreListedWithAndWithoutPrefix()
    {
        HiveMetastoreClient client = new SchemaMappingCoralMetastoreClient(
                new TestingHiveMetastoreClient(ImmutableList.of("egdp_prod_metrics_platform", "egdp_prod_supply", "default")),
                PREFIX);
        assertThat(client.getAllDatabases()).containsExactly(
                "egdp_prod_metrics_platform",
                "egdp_prod_supply",
                "default",
                "metrics_platform",
                "supply");
    }

    @Test
    public void testUnprefixedNameResolvesToPrefixedSchema()
    {
        TestingHiveMetastoreClient delegate = new TestingHiveMetastoreClient(ImmutableList.of("egdp_prod_metrics_platform"));
        HiveMetastoreClient client = new SchemaMappingCoralMetastoreClient(delegate, PREFIX);

        assertThat(client.getDatabase("metrics_platform").getName()).isEqualTo("egdp_prod_metrics_platform");
        assertThat(client.getAllTables("metrics_platform")).containsExactly("egdp_prod_metrics_platform.bookings");
        assertThat(client.getTable("metrics_platform", "bookings").getDbName()).isEqualTo("egdp_prod_metrics_platform");
    }

    @Test
    public void testPrefixedNameIsNotPrefixedTwice()
    {
        TestingHiveMetastoreClient delegate = new TestingHiveMetastoreClient(ImmutableList.of("egdp_prod_metrics_platform"));
        HiveMetastoreClient client = new SchemaMappingCoralMetastoreClient(delegate, PREFIX);

        assertThat(client.getDatabase("egdp_prod_metrics_platform").getName()).isEqualTo("egdp_prod_metrics_platform");
        assertThat(client.getTable("egdp_prod_metrics_platform", "bookings").getDbName()).isEqualTo("egdp_prod_metrics_platform");
    }

    @Test
    public void testNameOfAnotherPrefixIsUnchanged()
    {
        TestingHiveMetastoreClient delegate = new TestingHiveMetastoreClient(ImmutableList.of("egdp_test_metrics_platform"));
        HiveMetastoreClient client = new SchemaMappingCoralMetastoreClient(delegate, PREFIX);

        assertThat(client.getDatabase("egdp_test_metrics_platform").getName()).isEqualTo("egdp_test_metrics_platform");
    }

    private static class TestingHiveMetastoreClient
            implements HiveMetastoreClient
    {
        private final List<String> databases;

        private TestingHiveMetastoreClient(List<String> databases)
        {
            this.databases = ImmutableList.copyOf(databases);
        }

        @Override
        public List<String> getAllDatabases()
        {
            return databases;
        }

        @Override
        public Database getDatabase(String dbName)
        {
            Database database = new Database();
            database.setName(dbName);
            return database;
        }

        @Override
        public List<String> getAllTables(String dbName)
        {
            return ImmutableList.of(dbName + ".bookings");
        }

        @Override
        public Table getTable(String dbName, String tableName)
        {
            Table table = new Table();
            table.setDbName(dbName);
            table.setTableName(tableName);
            return table;
        }
    }
}
