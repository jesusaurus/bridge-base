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

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*studyId.*")
    public void nullStudyId() {
        new UploadSchemaKey.Builder().withSchemaId("test-schema").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*studyId.*")
    public void emptyStudyId() {
        new UploadSchemaKey.Builder().withStudyId("").withSchemaId("test-schema").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*studyId.*")
    public void blankStudyId() {
        new UploadSchemaKey.Builder().withStudyId("   ").withSchemaId("test-schema").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*schemaId.*")
    public void nullSchemaId() {
        new UploadSchemaKey.Builder().withStudyId("test-study").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*schemaId.*")
    public void emptySchemaId() {
        new UploadSchemaKey.Builder().withStudyId("test-study").withSchemaId("").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*schemaId.*")
    public void blankSchemaId() {
        new UploadSchemaKey.Builder().withStudyId("test-study").withSchemaId("   ").withRevision(42).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*revision.*")
    public void noRev() {
        new UploadSchemaKey.Builder().withStudyId("test-study").withSchemaId("test-schema").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*revision.*")
    public void negativeRev() {
        new UploadSchemaKey.Builder().withStudyId("test-study").withSchemaId("test-schema").withRevision(-1).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*revision.*")
    public void zeroRev() {
        new UploadSchemaKey.Builder().withStudyId("test-study").withSchemaId("test-schema").withRevision(0).build();
    }

    @Test
    public void happyCase() {
        UploadSchemaKey schemaKey = new UploadSchemaKey.Builder().withStudyId("test-study").withSchemaId("test-schema")
                .withRevision(42).build();
        assertEquals(schemaKey.getStudyId(), "test-study");
        assertEquals(schemaKey.getSchemaId(), "test-schema");
        assertEquals(schemaKey.getRevision(), 42);
        assertEquals(schemaKey.toString(), "test-study-test-schema-v42");
    }

    @Test
    public void jsonSerialize() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"studyId\":\"test-study\",\n" +
                "   \"schemaId\":\"test-schema\",\n" +
                "   \"revision\":42\n" +
                "}";

        // convert to POJO
        UploadSchemaKey schemaKey = DefaultObjectMapper.INSTANCE.readValue(jsonText, UploadSchemaKey.class);
        assertEquals(schemaKey.toString(), "test-study-test-schema-v42");

        // convert back to JSON
        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(schemaKey, JsonNode.class);
        assertEquals(jsonNode.get("studyId").textValue(), "test-study");
        assertEquals(jsonNode.get("schemaId").textValue(), "test-schema");
        assertEquals(jsonNode.get("revision").intValue(), 42);
    }
}
