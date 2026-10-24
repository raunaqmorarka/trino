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
package io.trino.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonMapperProvider;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAbstractTypedJacksonModule
{
    @Test
    public void testSerializerModifiersApplyToSubtypes()
            throws Exception
    {
        Set<Class<?>> modifiedClasses = ConcurrentHashMap.newKeySet();
        SimpleModule recordingModule = new SimpleModule("RecordingModule")
                .setSerializerModifier(new BeanSerializerModifier()
                {
                    @Override
                    public List<BeanPropertyWriter> changeProperties(SerializationConfig config, BeanDescription beanDescription, List<BeanPropertyWriter> beanProperties)
                    {
                        modifiedClasses.add(beanDescription.getBeanClass());
                        return beanProperties;
                    }
                });
        ObjectMapper mapper = new JsonMapperProvider()
                .withModules(ImmutableSet.of(recordingModule, new TestHandleModule()))
                .get();

        TestHandle handle = new TestHandle("table");
        String json = mapper.writeValueAsString(handle);

        assertThat(modifiedClasses).contains(TestHandle.class);
        assertThat(json).contains("\"@type\":\"test\"");
        assertThat(mapper.readValue(json, Handle.class)).isEqualTo(handle);
    }

    public interface Handle {}

    public static class TestHandle
            implements Handle
    {
        private final String name;

        @JsonCreator
        public TestHandle(@JsonProperty("name") String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @Override
        public boolean equals(Object other)
        {
            return other instanceof TestHandle that && name.equals(that.name);
        }

        @Override
        public int hashCode()
        {
            return name.hashCode();
        }
    }

    private static class TestHandleModule
            extends AbstractTypedJacksonModule<Handle>
    {
        public TestHandleModule()
        {
            super(Handle.class, _ -> "test", _ -> TestHandle.class);
        }
    }
}
