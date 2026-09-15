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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metastore.Column;
import io.trino.metastore.Table;
import io.trino.plugin.hive.ViewReaderUtil.PrestoViewReader;
import io.trino.plugin.hive.ViewReaderUtil.ViewReader;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.ViewReaderUtil.createViewReader;
import static io.trino.plugin.hive.ViewReaderUtil.encodeViewData;
import static io.trino.spi.connector.MetadataProvider.NOOP_METADATA_PROVIDER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSchemaMappingViewReader
{
    private static final SchemaMappingPrefixes PREFIXES = new SchemaMappingPrefixes(ImmutableSet.of("egdp_prod_", "egdp_test_"));
    private static final CatalogName CATALOG = new CatalogName("hive");

    @Test
    public void testViewBodyGetsPrefixOfItsOwnSchema()
    {
        ConnectorViewDefinition definition = decode("egdp_prod_metrics_platform", "SELECT id FROM metrics_platform.bookings");
        assertThat(definition.getOriginalSql()).isEqualTo("SELECT id FROM \"egdp_prod_metrics_platform\".bookings");
        assertThat(definition.getCatalog()).contains("hive");
        assertThat(definition.getSchema()).contains("egdp_prod_metrics_platform");
    }

    @Test
    public void testViewBodyReferencingAnotherSchemaGetsPrefix()
    {
        ConnectorViewDefinition definition = decode("egdp_prod_metrics_platform", "SELECT id FROM supply.lodging");
        assertThat(definition.getOriginalSql()).isEqualTo("SELECT id FROM \"egdp_prod_supply\".lodging");
    }

    @Test
    public void testPrefixedViewBodyIsUnchanged()
    {
        ConnectorViewDefinition definition = decode("egdp_prod_metrics_platform", "SELECT id FROM egdp_prod_supply.lodging");
        assertThat(definition.getOriginalSql()).isEqualTo("SELECT id FROM egdp_prod_supply.lodging");
    }

    @Test
    public void testUnqualifiedViewBodyIsUnchanged()
    {
        ConnectorViewDefinition definition = decode("egdp_prod_metrics_platform", "WITH bookings AS (SELECT 1 AS id) SELECT id FROM bookings");
        assertThat(definition.getOriginalSql()).isEqualTo("WITH bookings AS (SELECT 1 AS id) SELECT id FROM bookings");
    }

    @Test
    public void testViewOfUnmappedSchemaIsUnchanged()
    {
        ConnectorViewDefinition definition = decode("metrics_platform", "SELECT id FROM supply.lodging");
        assertThat(definition.getOriginalSql()).isEqualTo("SELECT id FROM supply.lodging");
        assertThat(definition.getCatalog()).contains("source_catalog");
        assertThat(definition.getSchema()).contains("metrics_platform");
    }

    @Test
    public void testCatalogQualifiedViewBodyGetsPrefix()
    {
        ConnectorViewDefinition definition = decode("egdp_prod_metrics_platform", "SELECT id FROM hive.supply.lodging");
        assertThat(definition.getOriginalSql()).isEqualTo("SELECT id FROM hive.\"egdp_prod_supply\".lodging");
    }

    @Test
    public void testViewBodyOfAnotherCatalogIsUnchanged()
    {
        ConnectorViewDefinition definition = decode("egdp_prod_metrics_platform", "SELECT id FROM other.supply.lodging");
        assertThat(definition.getOriginalSql()).isEqualTo("SELECT id FROM other.supply.lodging");
    }

    @Test
    public void testLegacyTranslatedViewBodyGetsPrefix()
    {
        Table table = Table.builder()
                .setDatabaseName("egdp_prod_metrics_platform")
                .setTableName("bookings_view")
                .setTableType(VIRTUAL_VIEW.name())
                .setOwner(Optional.empty())
                .setDataColumns(ImmutableList.of(new Column("id", HIVE_LONG, Optional.empty(), ImmutableMap.of())))
                .setViewExpandedText(Optional.of("SELECT `id` FROM `supply`.`lodging`"))
                .withStorage(storage -> storage.setStorageFormat(TEXTFILE.toStorageFormat()))
                .build();
        ConnectorSession session = getHiveSession(new HiveConfig().setLegacyHiveViewTranslation(true));
        ViewReader reader = createViewReader(
                null,
                session,
                table,
                TESTING_TYPE_MANAGER,
                (_, _) -> Optional.empty(),
                NOOP_METADATA_PROVIDER,
                false,
                DEFAULT_PRECISION,
                PREFIXES);

        ConnectorViewDefinition definition = reader.decodeViewData(Optional.empty(), table, CATALOG);
        assertThat(definition.getOriginalSql()).isEqualTo("SELECT \"id\" FROM \"egdp_prod_supply\".\"lodging\"");
        assertThat(definition.getSchema()).contains("egdp_prod_metrics_platform");
    }

    private static ConnectorViewDefinition decode(String schemaName, String sql)
    {
        ConnectorViewDefinition definition = new ConnectorViewDefinition(
                sql,
                Optional.of("source_catalog"),
                Optional.of("metrics_platform"),
                ImmutableList.of(new ViewColumn("id", BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of("owner"),
                false,
                ImmutableList.of());
        ViewReader reader = SchemaMappingViewReader.wrap(new PrestoViewReader(), PREFIXES, schemaName);
        return reader.decodeViewData(Optional.of(encodeViewData(definition)), viewTable(schemaName), CATALOG);
    }

    private static Table viewTable(String schemaName)
    {
        return Table.builder()
                .setDatabaseName(schemaName)
                .setTableName("bookings_view")
                .setTableType(VIRTUAL_VIEW.name())
                .setOwner(Optional.empty())
                .setParameters(ImmutableMap.of(PRESTO_VIEW_FLAG, "true"))
                .withStorage(storage -> storage.setStorageFormat(TEXTFILE.toStorageFormat()))
                .build();
    }
}
