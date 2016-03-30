package org.sagebionetworks.bridge.dynamodb;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.datapipeline.model.Field;
import com.amazonaws.services.datapipeline.model.PipelineObject;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;

import static org.testng.Assert.*;

/**
 * Created by liujoshua on 3/18/16.
 */
public class DynamoDataPipelineHelperTest {
    @Test
    public void testGetDDBSourceTable() {
        String tableName = "TableX";
        String throughput = "0.1";

        PipelineObject ddbSourceTable = DynamoDataPipelineHelper.getDDBSourceTable(tableName, throughput);

        assertEquals("DynamoDBDataNode", getFieldByKey(ddbSourceTable, "type").getStringValue());
        assertEquals(tableName, getFieldByKey(ddbSourceTable, "tableName").getStringValue());
        assertEquals(throughput, getFieldByKey(ddbSourceTable, "readThroughputPercent").getStringValue());
    }

    @Test
    public void testGetS3BackupLocation() {
        String s3Bucket = "s3BucketX";
        Region region = Region.getRegion(Regions.DEFAULT_REGION);
        String tableName = "tableX";

        PipelineObject s3BackupLocation = DynamoDataPipelineHelper.getS3BackupLocation(s3Bucket, region, tableName);

        assertEquals("S3DataNode", getFieldByKey(s3BackupLocation, "type").getStringValue());

        String s3Path = getFieldByKey(s3BackupLocation, "directoryPath").getStringValue();

        assertTrue(s3Path.startsWith("s3://" + s3Bucket + "/"));
        assertTrue(s3Path.contains(region.getName()));
        assertTrue(s3Path.contains(tableName));
    }

    @Test
    public void getEMRActivity() {
        String tableName = "TableX";
        TableDescription table = new TableDescription().withTableName(tableName);
        Region region = Region.getRegion(Regions.DEFAULT_REGION);

        String clusterId = "clusterX";
        String bucket = "bucketX";

        PipelineObject cluster = new PipelineObject().withId(clusterId);
        List<PipelineObject> objects = DynamoDataPipelineHelper.getEMRActivity(table, cluster, region, bucket);

        assertEquals(3, objects.size());

        PipelineObject s3Output = null;
        PipelineObject dynamoInput = null;
        PipelineObject emrActivity = null;

        for (PipelineObject object : objects) {
            String type = getFieldByKey(object, "type").getStringValue();

            switch (type) {
                case "EmrActivity":
                    emrActivity = object;
                    break;
                case "S3DataNode":
                    s3Output = object;
                    break;
                case "DynamoDBDataNode":
                    dynamoInput = object;
                    break;
            }
        }

        assertNotNull(dynamoInput);
        assertNotNull(emrActivity);
        assertNotNull(s3Output);

        assertEquals(dynamoInput.getId(), getFieldByKey(emrActivity, "input").getRefValue());
        assertEquals(s3Output.getId(), getFieldByKey(emrActivity, "output").getRefValue());
        assertEquals(clusterId, getFieldByKey(emrActivity, "runsOn").getRefValue());
    }

    private Field getFieldByKey(PipelineObject object, String key) {
        return getKeyToField(object.getFields()).get(key);
    }

    private HashMap<String, Field> getKeyToField(List<Field> fields) {
        HashMap<String, Field> map = Maps.newHashMapWithExpectedSize(fields.size());

        for (Field field : fields) {
            map.put(field.getKey(), field);
        }
        return map;
    }
}
