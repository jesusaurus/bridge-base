package org.sagebionetworks.bridge.schema;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class UploadSchemaKeyTest {
    @Test
    public void equalsVerifier() {
        EqualsVerifier.forClass(UploadSchemaKey.class).allFieldsShouldBeUsed().verify();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*appId.*")
    public void nullAppId() {
        new UploadSchemaKey.Builder().withAppId(null).withSchemaId("test-schema").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*appId.*")
    public void emptyAppId() {
        new UploadSchemaKey.Builder().withAppId("").withSchemaId("test-schema").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*appId.*")
    public void blankAppId() {
        new UploadSchemaKey.Builder().withAppId("   ").withSchemaId("test-schema").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*schemaId.*")
    public void nullSchemaId() {
        new UploadSchemaKey.Builder().withAppId("test-app").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*schemaId.*")
    public void emptySchemaId() {
        new UploadSchemaKey.Builder().withAppId("test-app").withSchemaId("").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*schemaId.*")
    public void blankSchemaId() {
        new UploadSchemaKey.Builder().withAppId("test-app").withSchemaId("   ").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*revision.*")
    public void noRev() {
        new UploadSchemaKey.Builder().withAppId("test-app").withSchemaId("test-schema").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*revision.*")
    public void negativeRev() {
        new UploadSchemaKey.Builder().withAppId("test-app").withSchemaId("test-schema").withRevision(-1).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*revision.*")
    public void zeroRev() {
        new UploadSchemaKey.Builder().withAppId("test-app").withSchemaId("test-schema").withRevision(0).build();
    }

    @Test
    public void happyCase() {
        UploadSchemaKey schemaKey = new UploadSchemaKey.Builder().withAppId("test-app").withSchemaId("test-schema")
                .withRevision(42).build();
        assertEquals(schemaKey.getAppId(), "test-app");
        assertEquals(schemaKey.getSchemaId(), "test-schema");
        assertEquals(schemaKey.getRevision(), 42);
        assertEquals(schemaKey.toString(), "test-app-test-schema-v42");
    }

    @Test
    public void jsonSerializeWithStudyId() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"studyId\":\"test-app\",\n" +
                "   \"schemaId\":\"test-schema\",\n" +
                "   \"revision\":42\n" +
                "}";

        // convert to POJO
        UploadSchemaKey schemaKey = DefaultObjectMapper.INSTANCE.readValue(jsonText, UploadSchemaKey.class);
        assertEquals(schemaKey.toString(), "test-app-test-schema-v42");

        // convert back to JSON
        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(schemaKey, JsonNode.class);
        assertEquals(jsonNode.get("appId").textValue(), "test-app");
        assertEquals(jsonNode.get("schemaId").textValue(), "test-schema");
        assertEquals(jsonNode.get("revision").intValue(), 42);
    }
    
    @Test
    public void jsonSerializeWithAppId() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"appId\":\"test-app\",\n" +
                "   \"schemaId\":\"test-schema\",\n" +
                "   \"revision\":42\n" +
                "}";

        // convert to POJO
        UploadSchemaKey schemaKey = DefaultObjectMapper.INSTANCE.readValue(jsonText, UploadSchemaKey.class);
        assertEquals(schemaKey.toString(), "test-app-test-schema-v42");

        // convert back to JSON
        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(schemaKey, JsonNode.class);
        assertEquals(jsonNode.get("appId").textValue(), "test-app");
        assertEquals(jsonNode.get("schemaId").textValue(), "test-schema");
        assertEquals(jsonNode.get("revision").intValue(), 42);
    }
}
