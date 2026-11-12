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

import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.DynamicFilters.collapseConsecutiveValues;
import static io.trino.sql.DynamicFilters.unionConsecutiveValues;
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

    @Test
    public void testUnionConsecutiveValues()
    {
        assertThat(unionConsecutiveValues(ImmutableList.of(multipleValues(BIGINT, ImmutableList.of(1L, 3L, 5L)), multipleValues(BIGINT, ImmutableList.of(2L, 4L)))))
                .contains(Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 5L, true)), false));
        assertThat(unionConsecutiveValues(ImmutableList.of(multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)), multipleValues(INTEGER, ImmutableList.of(2L, 3L, 4L), true))))
                .contains(Domain.create(ValueSet.ofRanges(range(INTEGER, 1L, true, 4L, true)), true));
        assertThat(unionConsecutiveValues(ImmutableList.of(evenValues(0, 128), oddValues(0, 128), Domain.none(BIGINT))))
                .contains(Domain.create(ValueSet.ofRanges(range(BIGINT, 0L, true, 127L, true)), false));
        assertThat(unionConsecutiveValues(ImmutableList.of(evenValues(0, 130), oddValues(0, 130))))
                .contains(Domain.create(ValueSet.ofRanges(range(BIGINT, 0L, true, 129L, true)), false));
        assertThat(unionConsecutiveValues(ImmutableList.of(evenValues(0, 130), oddValues(0, 128), multipleValues(BIGINT, ImmutableList.of(0L, 130L)))))
                .isEmpty();
        assertThat(unionConsecutiveValues(ImmutableList.of(multipleValues(DATE, ImmutableList.of(1L, 3L)), multipleValues(DATE, ImmutableList.of(3L, 5L, 2L)))))
                .isEmpty();
        assertThat(unionConsecutiveValues(ImmutableList.of(singleValue(BIGINT, Long.MIN_VALUE), singleValue(BIGINT, Long.MAX_VALUE))))
                .isEmpty();
        assertThat(unionConsecutiveValues(ImmutableList.of(multipleValues(BIGINT, ImmutableList.of(1L, 3L)), Domain.create(ValueSet.ofRanges(range(BIGINT, 2L, true, 4L, true)), false))))
                .isEmpty();
        assertThat(unionConsecutiveValues(ImmutableList.of(Domain.none(BIGINT), Domain.onlyNull(BIGINT))))
                .isEmpty();
        assertThat(unionConsecutiveValues(ImmutableList.of(multipleValues(VARCHAR, ImmutableList.of(utf8Slice("a"), utf8Slice("b"))))))
                .isEmpty();
    }

    private static Domain evenValues(long start, long end)
    {
        return multipleValues(BIGINT, LongStream.range(start, end).filter(value -> value % 2 == 0).boxed().collect(toImmutableList()));
    }

    private static Domain oddValues(long start, long end)
    {
        return multipleValues(BIGINT, LongStream.range(start, end).filter(value -> value % 2 == 1).boxed().collect(toImmutableList()));
    }
}
