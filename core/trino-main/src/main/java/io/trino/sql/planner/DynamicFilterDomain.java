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
package io.trino.sql.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DynamicFilterDomain
{
    private final Optional<Domain> domain;
    private final Optional<BitmapFilter> bitmap;

    private DynamicFilterDomain(Optional<Domain> domain, Optional<BitmapFilter> bitmap)
    {
        this.domain = requireNonNull(domain, "domain is null");
        this.bitmap = requireNonNull(bitmap, "roaringBitmap is null");
        checkArgument(domain.isPresent() != bitmap.isPresent(), "Exactly one of domain and roaringBitmap must be present");
    }

    @JsonCreator
    public static DynamicFilterDomain create(
            @JsonProperty("domain") Optional<Domain> domain,
            @JsonProperty("bitmap") Optional<BitmapFilter> bitmap)
    {
        return new DynamicFilterDomain(domain, bitmap);
    }

    public DynamicFilterDomain(Domain domain)
    {
        this(Optional.of(domain), Optional.empty());
    }

    public DynamicFilterDomain(Roaring64Bitmap bitmap, Type type, boolean nullAllowed)
    {
        this(Optional.empty(), Optional.of(new BitmapFilter(bitmap, type, nullAllowed)));
    }

    public DynamicFilterDomain(BitmapFilter bitmap)
    {
        this(Optional.empty(), Optional.of(bitmap));
    }

    @JsonProperty
    public Optional<Domain> getDomain()
    {
        return domain;
    }

    @JsonProperty
    public Optional<BitmapFilter> getBitmap()
    {
        return bitmap;
    }

    @JsonIgnore
    public Type getType()
    {
        return domain.map(Domain::getType).orElseGet(() -> bitmap.orElseThrow().type());
    }

    public Domain toDomain()
    {
        return domain.orElseGet(() -> fromBitmap(bitmap.orElseThrow()));
    }

    @JsonIgnore
    public Range getSpan()
    {
        return domain.map(domainValue -> domainValue.getValues().getRanges().getSpan())
                .orElseGet(() -> {
                    Roaring64Bitmap roaringBitmap = bitmap.orElseThrow().roaringBitmap();
                    return Range.range(bitmap.get().type(), roaringBitmap.first(), true, roaringBitmap.last(), true);
                });
    }

    public boolean isAll()
    {
        return domain.map(Domain::isAll).orElse(false);
    }

    public boolean isNone()
    {
        return domain.map(Domain::isNone).orElseGet(() -> bitmap.orElseThrow().isNone());
    }

    public DynamicFilterDomain compact()
    {
        if (bitmap.isPresent()) {
            bitmap.get().roaringBitmap().runOptimize();
            bitmap.get().roaringBitmap().trim();
        }
        return this;
    }

    public DynamicFilterDomain simplify(int threshold)
    {
        if (domain.isPresent()) {
            return new DynamicFilterDomain(domain.get().simplify(threshold));
        }
        return new DynamicFilterDomain(fromBitmap(bitmap.orElseThrow()));
    }

    public DynamicFilterDomain union(DynamicFilterDomain other)
    {
        if (domain.isPresent() && other.domain.isPresent()) {
            return new DynamicFilterDomain(domain.get().union(other.domain.get()));
        }
        if (bitmap.isPresent() && other.bitmap.isPresent()) {
            Roaring64Bitmap newBitmap = bitmap.get().roaringBitmap();
            newBitmap.or(other.bitmap.get().roaringBitmap());
            return new DynamicFilterDomain(newBitmap, bitmap.get().type(), bitmap.get().nullAllowed() || other.bitmap.get().nullAllowed());
        }
        if (domain.isPresent()) {
            return union(other.bitmap.orElseThrow(), domain.get());
        }
        return union(bitmap.orElseThrow(), other.domain.orElseThrow());
    }

    public DynamicFilterDomain intersect(DynamicFilterDomain other)
    {
        if (domain.isPresent() && other.domain.isPresent()) {
            return new DynamicFilterDomain(domain.get().intersect(other.domain.get()));
        }
        if (bitmap.isPresent() && other.bitmap.isPresent()) {
            Roaring64Bitmap newBitmap = bitmap.get().roaringBitmap();
            newBitmap.and(other.bitmap.get().roaringBitmap());
            return new DynamicFilterDomain(newBitmap, bitmap.get().type(), bitmap.get().nullAllowed() && other.bitmap.get().nullAllowed());
        }
        if (domain.isPresent()) {
            return intersect(other.bitmap.orElseThrow(), domain.get());
        }
        return intersect(bitmap.orElseThrow(), other.domain.orElseThrow());
    }

    public long getRetainedSizeInBytes()
    {
        return domain.map(Domain::getRetainedSizeInBytes).orElse(0L) +
                bitmap.map(BitmapFilter::getRetainedSizeInBytes).orElse(0L);
    }

    public String toString(ConnectorSession connectorSession, int limit)
    {
        if (isAll()) {
            return "ALL";
        }
        if (isNone()) {
            return "NONE";
        }
        if (domain.isPresent()) {
            return domain.get().toString(connectorSession, limit);
        }
        return bitmap.get().toString();
    }

    public DynamicFilterDomain withNullsAllowed()
    {
        if (domain.isPresent()) {
            return new DynamicFilterDomain(Domain.create(domain.get().getValues(), true));
        }
        return new DynamicFilterDomain(new BitmapFilter(bitmap.orElseThrow().roaringBitmap(), bitmap.orElseThrow().type(), true));
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamicFilterDomain that = (DynamicFilterDomain) o;
        return Objects.equals(domain, that.domain) && Objects.equals(bitmap, that.bitmap);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(domain, bitmap);
    }

    public static DynamicFilterDomain onlyNull(Type type)
    {
        return new DynamicFilterDomain(Domain.onlyNull(type));
    }

    public static DynamicFilterDomain singleValue(Type type, Object value)
    {
        return new DynamicFilterDomain(Domain.singleValue(type, value, false));
    }

    public static DynamicFilterDomain multipleValues(Type type, List<?> values)
    {
        return new DynamicFilterDomain(Domain.multipleValues(type, values, false));
    }

    public static DynamicFilterDomain union(List<DynamicFilterDomain> domains)
    {
        if (domains.isEmpty()) {
            throw new IllegalArgumentException("domains cannot be empty for union");
        }
        if (domains.size() == 1) {
            return domains.get(0);
        }
        List<Domain> domainsToMerge = domains.stream()
                .map(DynamicFilterDomain::getDomain)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
        Domain mergedDomain = domainsToMerge.isEmpty() ? Domain.none(domains.get(0).getType()) : Domain.union(domainsToMerge);

        List<BitmapFilter> bitmapsToMerge = domains.stream()
                .map(DynamicFilterDomain::getBitmap)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
        if (bitmapsToMerge.isEmpty()) {
            return new DynamicFilterDomain(mergedDomain);
        }
        BitmapFilter mergedBitmap = BitmapFilter.union(bitmapsToMerge);
        return union(mergedBitmap, mergedDomain);
    }

    public static DynamicFilterDomain all(Type type)
    {
        return new DynamicFilterDomain(Domain.all(type));
    }

    public static DynamicFilterDomain none(Type type)
    {
        return new DynamicFilterDomain(Domain.none(type));
    }

    private static DynamicFilterDomain union(BitmapFilter bitmap, Domain domain)
    {
        if (domain.isNone()) {
            return new DynamicFilterDomain(bitmap);
        }
        if (bitmap.isNone() || domain.isAll()) {
            return new DynamicFilterDomain(domain);
        }
        if (domain.isNullableDiscreteSet()) {
            for (Object value : domain.getNullableDiscreteSet().getNonNullValues()) {
                bitmap.roaringBitmap().addLong((long) value);
            }
            return new DynamicFilterDomain(bitmap.roaringBitmap(), bitmap.type(), domain.isNullAllowed() || bitmap.nullAllowed());
        }

        Domain bitmapSpan = fromBitmap(bitmap);
        return new DynamicFilterDomain(bitmapSpan.union(domain));
    }

    private static DynamicFilterDomain intersect(BitmapFilter bitmap, Domain domain)
    {
        if (domain.isNone() || bitmap.isNone()) {
            return new DynamicFilterDomain(Domain.none(domain.getType()));
        }
        if (domain.isAll()) {
            return new DynamicFilterDomain(bitmap);
        }
        if (domain.isNullableDiscreteSet()) {
            Roaring64Bitmap newBitmap = new Roaring64Bitmap();
            for (Object value : domain.getNullableDiscreteSet().getNonNullValues()) {
                if (bitmap.roaringBitmap().contains((long) value)) {
                    newBitmap.addLong((long) value);
                }
            }
            return new DynamicFilterDomain(newBitmap, bitmap.type(), domain.isNullAllowed() && bitmap.nullAllowed());
        }
        Domain bitmapSpan = fromBitmap(bitmap);
        return new DynamicFilterDomain(bitmapSpan.intersect(domain));
    }

    private static Domain fromBitmap(BitmapFilter bitmap)
    {
        return Domain.create(
                ValueSet.ofRanges(Range.range(bitmap.type(), bitmap.roaringBitmap().first(), true, bitmap.roaringBitmap().last(), true)),
                bitmap.nullAllowed());
    }
}
