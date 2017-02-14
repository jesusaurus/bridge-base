package org.sagebionetworks.bridge.dynamodb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.dynamodb.test.GlobalIndexTest;

public class AnnotationBasedTableCreatorTest {
    @Test
    public void testGlobalIndices() {
        // Our test class is GlobalIndexTest, which has a bunch of contrived indices.
        List<TableDescription> tableDescList = DynamoTestUtils.MAPPER.getTables(GlobalIndexTest.class);
        assertEquals(tableDescList.size(), 1);

        TableDescription tableDesc = tableDescList.get(0);
        assertTrue(tableDesc.getTableName().endsWith("GlobalIndexTest"));

        // There are 7 indices, though due to how Reflection works, we can't be sure what order they'll be in.
        // Convert to a map so we can validate each index.
        List<GlobalSecondaryIndexDescription> globalIndexDescList = tableDesc.getGlobalSecondaryIndexes();
        assertEquals(globalIndexDescList.size(), 7);

        Map<String, GlobalSecondaryIndexDescription> indexDescByName = globalIndexDescList.stream()
                .collect(Collectors.toMap(GlobalSecondaryIndexDescription::getIndexName, index -> index));
        assertEquals(indexDescByName.size(), 7);

        // index with hash key and no range key
        validateGlobalIndexDesc(indexDescByName.get("hash-without-range-index"), "hashWithoutRangeHashKey", null);

        // index with non-list annotations
        validateGlobalIndexDesc(indexDescByName.get("simple-index"), "simpleIndexHashKey", "simpleIndexRangeKey");

        // indices with list annotations (a, b) * (x, y) = (ax, ay, bx, by)
        validateGlobalIndexDesc(indexDescByName.get("ax-list-index"), "listIndexHashKeyA", "listIndexRangeKeyX");
        validateGlobalIndexDesc(indexDescByName.get("ay-list-index"), "listIndexHashKeyA", "listIndexRangeKeyY");
        validateGlobalIndexDesc(indexDescByName.get("bx-list-index"), "listIndexHashKeyB", "listIndexRangeKeyX");
        validateGlobalIndexDesc(indexDescByName.get("by-list-index"), "listIndexHashKeyB", "listIndexRangeKeyY");

        // index with list annotation with hash key only
        validateGlobalIndexDesc(indexDescByName.get("a-only-list-index"), "listIndexHashKeyA", null);
    }

    private static void validateGlobalIndexDesc(GlobalSecondaryIndexDescription indexDesc, String hashKey,
            String rangeKey) {
        List<KeySchemaElement> keySchemaList = indexDesc.getKeySchema();

        // There's always a hash key.
        assertEquals(keySchemaList.get(0).getAttributeName(), hashKey);
        assertEquals(keySchemaList.get(0).getKeyType(), "HASH");

        if (rangeKey == null) {
            // No range key. Key schema list has 1 element.
            assertEquals(keySchemaList.size(), 1);
        } else {
            // Yes range key. Key schema list has 2 elements. Validate the range key.
            assertEquals(keySchemaList.size(), 2);
            assertEquals(keySchemaList.get(1).getAttributeName(), rangeKey);
            assertEquals(keySchemaList.get(1).getKeyType(), "RANGE");
        }
    }
}
