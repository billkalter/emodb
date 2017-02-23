package com.bazaarvoice.emodb.common.json;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.util.VersionUtil;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.std.MapSerializer;
import com.fasterxml.jackson.databind.type.MapType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.dropwizard.jackson.Jackson;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class HiddenJsonViewsTest {

    // JSON view class to show hidden fields
    public static class ShowHiddenFields {}

    // Implemented as a Jackson module because it's best suited for easy inclusion in an object mapper.
    private abstract class HiddenJsonViewsModule extends Module {
        @Override
        public String getModuleName() {
            return "HiddenJsonViews";
        }

        @Override
        public Version version() {
            return VersionUtil.parseVersion("1.0", "com.bazaarvoice.emodb.common.json", "dynamic-hidden");
        }

        @Override
        public void setupModule(SetupContext context) {
            context.addBeanSerializerModifier(new BeanSerializerModifier() {
                @Override
                public JsonSerializer<?> modifyMapSerializer(SerializationConfig config, MapType valueType, BeanDescription beanDesc,
                                                             JsonSerializer<?> serializer) {
                    return createMapSerializer((MapSerializer) serializer);
                }

                private MapSerializer createMapSerializer(final MapSerializer original) {
                    return new MapSerializer(original, null, false) {
                        @Override
                        public void serialize(Map<?, ?> value, JsonGenerator jgen, SerializerProvider provider)
                                throws IOException, JsonGenerationException {
                            // If the JSON view is not ShowHiddenFields then hidden fields are obscured
                            if (provider.getActiveView() == null || !ShowHiddenFields.class.isAssignableFrom(provider.getActiveView())) {
                                value = Maps.filterKeys(value, HiddenJsonViewsModule.this::isNotHiddenKey);
                            }
                            super.serialize(value, jgen, provider);
                        }

                        /**
                         * Overriding contextual creation is required, otherwise the overriding behavior is lost.
                         */
                        @Override
                        public JsonSerializer<?> createContextual(SerializerProvider provider, BeanProperty property)
                                throws JsonMappingException {
                            MapSerializer contextual = (MapSerializer) original.createContextual(provider, property);
                            if (contextual == original) {
                                return this;
                            }

                            return createMapSerializer(contextual);
                        }

                    };
                }
            });
        }

        /**
         * Abstracted to demonstrate two possible approaches: dynamic prefixes and static hidden fields
         */
        abstract boolean isNotHiddenKey(Object key);
    }

    @Test
    public void testHiddenJsonViewsWithDynamicFields() throws Exception {
        ObjectMapper objectMapper = Jackson.newObjectMapper()
                .registerModule(new HiddenJsonViewsModule() {
                    @Override
                    protected boolean isNotHiddenKey(Object key) {
                        // Just being safe here; we know all keys used by Emo are non-null Strings
                        return key == null || !(key instanceof String) || !((String) key).startsWith("~hidden.");
                    }
                });

        Map<String, Object> map = ImmutableMap.of("visible", "public", "~hidden.x", "private");

        // Normal
        String json = objectMapper.writeValueAsString(map);
        assertEquals(objectMapper.readValue(json, Map.class), ImmutableMap.of("visible", "public"));

        // Hidden
        json = objectMapper.writerWithView(ShowHiddenFields.class).writeValueAsString(map);
        assertEquals(objectMapper.readValue(json, Map.class), ImmutableMap.of("visible", "public", "~hidden.x", "private"));
    }

    @Test
    public void testHiddenJsonViewsWithStaticField() throws Exception {
        ObjectMapper objectMapper = Jackson.newObjectMapper()
                .registerModule(new HiddenJsonViewsModule() {
                    @Override
                    protected boolean isNotHiddenKey(Object key) {
                        return !"~hidden".equals(key);
                    }
                });

        Map<String, Object> map = ImmutableMap.of("visible", "public", "~hidden", "private");

        // Normal
        String json = objectMapper.writeValueAsString(map);
        assertEquals(objectMapper.readValue(json, Map.class), ImmutableMap.of("visible", "public"));

        // Hidden
        json = objectMapper.writerWithView(ShowHiddenFields.class).writeValueAsString(map);
        assertEquals(objectMapper.readValue(json, Map.class), ImmutableMap.of("visible", "public", "~hidden", "private"));
    }
}
