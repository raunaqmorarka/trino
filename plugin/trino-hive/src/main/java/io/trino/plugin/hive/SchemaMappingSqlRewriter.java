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

import io.trino.plugin.hive.SchemaMappingPrefixes.SchemaPrefix;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Locale.ENGLISH;

/**
 * Qualifies schema names in a view body with the schema mapping prefix the view is stored under.
 */
public final class SchemaMappingSqlRewriter
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private SchemaMappingSqlRewriter() {}

    /**
     * Resolves the view body in the catalog the view is read from, with schema names carrying the prefix.
     */
    public static ConnectorViewDefinition prefixSchemaNames(ConnectorViewDefinition definition, SchemaPrefix prefix, CatalogName catalogName)
    {
        Set<String> mappedCatalogs = Stream.concat(Stream.of(catalogName.toString()), definition.getCatalog().stream())
                .collect(toImmutableSet());
        return new ConnectorViewDefinition(
                prefixSchemaNames(definition.getOriginalSql(), prefix, mappedCatalogs),
                Optional.of(catalogName.toString()),
                definition.getSchema().map(prefix::apply),
                definition.getColumns(),
                definition.getComment(),
                definition.getOwner(),
                definition.isRunAsInvoker(),
                definition.getPath());
    }

    /**
     * Adds the prefix to every schema name of a table reference which does not already carry one.
     * A three part table reference is rewritten only when its catalog is one of {@code mappedCatalogs}.
     */
    public static String prefixSchemaNames(String sql, SchemaPrefix prefix, Set<String> mappedCatalogs)
    {
        Statement statement = SQL_PARSER.createStatement(sql);
        List<Identifier> schemaNames = new ArrayList<>();
        new SchemaNameCollector(mappedCatalogs).process(statement, schemaNames);

        List<Identifier> rewritten = schemaNames.stream()
                .filter(schemaName -> !prefix.apply(identifierValue(schemaName)).equals(identifierValue(schemaName)))
                .sorted(Comparator.comparing(SchemaMappingSqlRewriter::locationOf, Comparator
                                .comparingInt(NodeLocation::getLineNumber)
                                .thenComparingInt(NodeLocation::getColumnNumber))
                        .reversed())
                .collect(toImmutableList());

        StringBuilder result = new StringBuilder(sql);
        for (Identifier schemaName : rewritten) {
            int start = offsetOf(sql, locationOf(schemaName));
            int end = start + sourceText(schemaName).length();
            verify(end <= sql.length() && sql.substring(start, end).equalsIgnoreCase(sourceText(schemaName)),
                    "Schema name %s not found at %s in: %s",
                    schemaName,
                    locationOf(schemaName),
                    sql);
            result.replace(start, end, quoted(prefix.apply(identifierValue(schemaName))));
        }
        return result.toString();
    }

    private static String identifierValue(Identifier identifier)
    {
        if (identifier.isDelimited()) {
            return identifier.getValue();
        }
        return identifier.getValue().toLowerCase(ENGLISH);
    }

    private static String sourceText(Identifier identifier)
    {
        if (identifier.isDelimited()) {
            return quoted(identifier.getValue());
        }
        return identifier.getValue();
    }

    private static String quoted(String value)
    {
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    private static NodeLocation locationOf(Identifier identifier)
    {
        return identifier.getLocation().orElseThrow(() -> new IllegalArgumentException("Identifier has no location: " + identifier));
    }

    private static int offsetOf(String sql, NodeLocation location)
    {
        int offset = 0;
        for (int line = 1; line < location.getLineNumber(); line++) {
            int lineEnd = sql.indexOf('\n', offset);
            verify(lineEnd >= 0, "Line %s not found in: %s", location.getLineNumber(), sql);
            offset = lineEnd + 1;
        }
        return offset + location.getColumnNumber() - 1;
    }

    private static class SchemaNameCollector
            extends DefaultTraversalVisitor<List<Identifier>>
    {
        private final Set<String> mappedCatalogs;

        public SchemaNameCollector(Set<String> mappedCatalogs)
        {
            this.mappedCatalogs = mappedCatalogs.stream()
                    .map(catalog -> catalog.toLowerCase(ENGLISH))
                    .collect(toImmutableSet());
        }

        @Override
        protected Void visitTable(Table node, List<Identifier> schemaNames)
        {
            schemaOf(node).ifPresent(schemaNames::add);
            return super.visitTable(node, schemaNames);
        }

        private Optional<Identifier> schemaOf(Table node)
        {
            List<Identifier> parts = node.getName().getOriginalParts();
            if (parts.size() == 2) {
                return Optional.of(parts.get(0));
            }
            if (parts.size() == 3 && mappedCatalogs.contains(identifierValue(parts.get(0)))) {
                return Optional.of(parts.get(1));
            }
            return Optional.empty();
        }
    }
}
