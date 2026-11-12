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
package io.trino.sql;

import com.google.common.collect.ImmutableList;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.DynamicFilters.collapseConsecutiveValues;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicFilters
{
    @Test
    public void testCollapseConsecutiveValues()
    {
        assertThat(collapseConsecutiveValues(multipleValues(BIGINT, ImmutableList.of(3L, 4L, 5L))))
                .isEqualTo(Domain.create(ValueSet.ofRanges(range(BIGINT, 3L, true, 5L, true)), false));
        assertThat(collapseConsecutiveValues(multipleValues(DATE, ImmutableList.of(10L, 11L), true)))
                .isEqualTo(Domain.create(ValueSet.ofRanges(range(DATE, 10L, true, 11L, true)), true));
        assertThat(collapseConsecutiveValues(multipleValues(INTEGER, ImmutableList.of(1L, 2L, 4L))))
                .isEqualTo(multipleValues(INTEGER, ImmutableList.of(1L, 2L, 4L)));
        assertThat(collapseConsecutiveValues(multipleValues(INTEGER, ImmutableList.of((long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE))))
                .isEqualTo(multipleValues(INTEGER, ImmutableList.of((long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE)));
        assertThat(collapseConsecutiveValues(multipleValues(BIGINT, ImmutableList.of(Long.MIN_VALUE, Long.MAX_VALUE))))
                .isEqualTo(multipleValues(BIGINT, ImmutableList.of(Long.MIN_VALUE, Long.MAX_VALUE)));
        assertThat(collapseConsecutiveValues(multipleValues(VARCHAR, ImmutableList.of(utf8Slice("a"), utf8Slice("b")))))
                .isEqualTo(multipleValues(VARCHAR, ImmutableList.of(utf8Slice("a"), utf8Slice("b"))));
        assertThat(collapseConsecutiveValues(Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 2L, true), range(BIGINT, 3L, true, 4L, true)), false)))
                .isEqualTo(Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 2L, true), range(BIGINT, 3L, true, 4L, true)), false));
    }
}
