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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.errorprone.annotations.DoNotCall;
import io.airlift.units.DataSize;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.util.FastutilSetHelper.isDirectLongComparisonValidType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public record BitmapFilter(Roaring64Bitmap roaringBitmap, Type type, boolean nullAllowed)
{
    public BitmapFilter
    {
        requireNonNull(roaringBitmap, "roaringBitmap is null");
        requireNonNull(type, "type is null");
        checkArgument(type.isOrderable(), "Type must be orderable");
        checkArgument(type.getJavaType() == long.class, "Type must be of long class");
    }

    public boolean isNone()
    {
        return roaringBitmap.isEmpty() && !nullAllowed;
    }

    @UsedByGeneratedCode
    public boolean test(Block block, int position)
    {
        if (block.isNull(position)) {
            return nullAllowed;
        }
        long value = type.getLong(block, position);
        return roaringBitmap.contains(value);
    }

    public long getRetainedSizeInBytes()
    {
        return roaringBitmap.getLongSizeInBytes();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BitmapFilter that = (BitmapFilter) o;
        return nullAllowed == that.nullAllowed
                && Objects.equals(type, that.type)
                && Objects.equals(roaringBitmap, that.roaringBitmap);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(roaringBitmap, type, nullAllowed);
    }

    @Override
    public String toString()
    {
        return format(
                "[%s Bitmap cardinality: %s, size: %s ]",
                nullAllowed ? " NULL," : "",
                roaringBitmap.getLongCardinality(),
                DataSize.succinctBytes(roaringBitmap.getLongSizeInBytes()));
    }

    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static BitmapFilter fromJson(
            @JsonProperty("roaringBitmap") Roaring64Bitmap roaringBitmap,
            @JsonProperty("type") Type type,
            @JsonProperty("nullAllowed") boolean nullAllowed)
    {
        return new BitmapFilter(roaringBitmap, type, nullAllowed);
    }

    @JsonProperty
    @Override
    public Roaring64Bitmap roaringBitmap()
    {
        return roaringBitmap;
    }

    @JsonProperty
    @Override
    public Type type()
    {
        return type;
    }

    @JsonProperty
    @Override
    public boolean nullAllowed()
    {
        return nullAllowed;
    }

    public static BitmapFilter union(List<BitmapFilter> bitmaps)
    {
        if (bitmaps.isEmpty()) {
            throw new IllegalArgumentException("bitmaps cannot be empty for union");
        }
        if (bitmaps.size() == 1) {
            return bitmaps.get(0);
        }
        Roaring64Bitmap result = new Roaring64Bitmap();
        for (BitmapFilter bitmap : bitmaps) {
            result.or(bitmap.roaringBitmap());
        }
        return new BitmapFilter(result, bitmaps.get(0).type(), bitmaps.stream().anyMatch(BitmapFilter::nullAllowed));
    }

    public static Optional<BitmapFilter> fromDomain(Domain domain)
    {
        if (!isDirectLongComparisonValidType(domain.getType()) || !domain.isNullableDiscreteSet()) {
            return Optional.empty();
        }
        Roaring64Bitmap roaringBitmap = new Roaring64Bitmap();
        for (Object value : domain.getNullableDiscreteSet().getNonNullValues()) {
            roaringBitmap.add((long) value);
        }
        roaringBitmap.runOptimize();
        roaringBitmap.trim();
        return Optional.of(new BitmapFilter(roaringBitmap, domain.getType(), domain.isNullAllowed()));
    }

    public static class BitmapDeserializer
            extends JsonDeserializer<Roaring64Bitmap>
    {
        @Override
        public Roaring64Bitmap deserialize(JsonParser jsonParser, DeserializationContext ctxt)
                throws IOException
        {
            byte[] data = jsonParser.getBinaryValue(Base64Variants.MIME_NO_LINEFEEDS);
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            DataInputStream bdi = new DataInputStream(bis);
            Roaring64Bitmap roaringBitmap = new Roaring64Bitmap();
            roaringBitmap.deserialize(bdi);
            return roaringBitmap;
        }
    }

    public static class BitmapSerializer
            extends JsonSerializer<Roaring64Bitmap>
    {
        @Override
        public void serialize(Roaring64Bitmap bitmap, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream bdo = new DataOutputStream(bos);
            bitmap.serialize(bdo);
            byte[] data = bos.toByteArray();
            jsonGenerator.writeBinary(Base64Variants.MIME_NO_LINEFEEDS, data, 0, data.length);
        }
    }
}
