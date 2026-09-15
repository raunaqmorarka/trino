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

import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static java.util.Comparator.comparingInt;
import static java.util.Objects.requireNonNull;

/**
 * Schema name prefixes configured by schema mapping rules.
 */
public record SchemaMappingPrefixes(Set<String> prefixes)
{
    public static final SchemaMappingPrefixes NONE = new SchemaMappingPrefixes(ImmutableSet.of());

    public SchemaMappingPrefixes
    {
        prefixes = ImmutableSet.copyOf(requireNonNull(prefixes, "prefixes is null"));
    }

    /**
     * Returns the longest configured prefix of the schema name.
     */
    public Optional<SchemaPrefix> findPrefix(String schemaName)
    {
        return longestPrefixOf(schemaName).map(prefix -> new SchemaPrefix(prefix, this));
    }

    private Optional<String> longestPrefixOf(String schemaName)
    {
        return prefixes.stream()
                .filter(schemaName::startsWith)
                .max(comparingInt(String::length));
    }

    /**
     * A configured prefix, applied to the schema names of one delegate metastore.
     */
    public record SchemaPrefix(String value, SchemaMappingPrefixes prefixes)
    {
        public SchemaPrefix
        {
            requireNonNull(value, "value is null");
            requireNonNull(prefixes, "prefixes is null");
        }

        /**
         * Adds the prefix to the schema name unless it already carries a configured prefix.
         */
        public String apply(String schemaName)
        {
            if (prefixes.longestPrefixOf(schemaName).isPresent()) {
                return schemaName;
            }
            return value + schemaName;
        }
    }
}
