package org.sagebionetworks.bridge.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.testng.annotations.Test;

public class DefaultObjectMapperTest {

    private static class TestObject {
        @SuppressWarnings("unused")
        private String name;
        @SuppressWarnings("unused")
        private String value;
        @JsonCreator
        public TestObject(@JsonProperty("name") String name, 
                @JsonProperty("value") String value) {
            this.name = name;
            this.value = value;
        }
    }
    
    @Test
    public void silentlyIgnoresUnknownProperties() throws Exception {
        String json = "{\"name\": \"aName\", \"value\": \"aValue\", \"third\": \"unknownProperty\"}";
        DefaultObjectMapper.INSTANCE.readValue(json, TestObject.class);
    }
    
}
