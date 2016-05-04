package org.sagebionetworks.bridge.dynamodb;

import static org.sagebionetworks.bridge.dynamodb.DynamoDataPipelineHelper.getDDBSourceTable;
import static org.sagebionetworks.bridge.dynamodb.DynamoDataPipelineHelper.getEMRCluster;
import static org.sagebionetworks.bridge.dynamodb.DynamoDataPipelineHelper.getS3BackupLocation;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.datapipeline.model.Field;
import com.amazonaws.services.datapipeline.model.PipelineObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalTime;
import org.testng.annotations.Test;

/**
 * Created by liujoshua on 3/18/16.
 */
public class DynamoDataPipelineHelperTest {

    @Test
    public void testGetStringValue()   {
        PipelineObject o = new PipelineObject().withFields(new Field().withKey("keyA").withStringValue("valA"),
                new Field().withKey("keyB").withStringValue("valB"),
                new Field().withKey("keyC").withRefValue("valC")
                );

        assertEquals("valB", DynamoDataPipelineHelper.getStringValue(o, "keyB").get());
    }

    @Test
    public void testGetRefValue() throws Exception {
        PipelineObject o = new PipelineObject().withFields(new Field().withKey("keyA").withRefValue("valA"),
                new Field().withKey("keyB").withStringValue("valB"),
                new Field().withKey("keyC").withRefValue("valC")
        );

        assertEquals("valC", DynamoDataPipelineHelper.getRefValue(o, "keyC").get());
    }

    @Test
    public void testCreatePipelineObjects() {
        Region dynamoRegion = Region.getRegion(Regions.US_EAST_1);
        List<String> tableNames = Lists.newArrayList("Table1", "Table2");
        String s3Bucket = "Bucket";
        LocalTime localStartTime = new LocalTime(1, 0);
        DateTimeZone localDateTimeZone = DateTimeZone.forID("America/Los_Angeles");

        List<PipelineObject> objects = DynamoDataPipelineHelper.createPipelineObjects(dynamoRegion,
                tableNames,
                s3Bucket,
                localStartTime,
                localDateTimeZone
        );

        for (String tableName : tableNames) {

            List<PipelineObject> dynamoNodes = objects
                    .stream()
                    .filter(o->isOfType(o, DynamoDataPipelineHelper.PipelineObjectType.DYNAMO_DATA_NODE))
                    .filter(o -> tableName.equals(getFieldByKey(o, "tableName").getStringValue()))
                    .collect(Collectors.toList());
            assertEquals(1, dynamoNodes.size());

            List<PipelineObject> s3Outputs = objects
                    .stream()
                    .filter(o->isOfType(o, DynamoDataPipelineHelper.PipelineObjectType.S3_DATA_NODE))
                    .filter(o -> getFieldByKey(o, "directoryPath").getStringValue().contains(tableName))
                    .collect(Collectors.toList());
            assertEquals(1, s3Outputs.size());

            List<PipelineObject> activities = objects
                    .stream()
                    .filter(o->isOfType(o, DynamoDataPipelineHelper.PipelineObjectType.HADOOP_ACTIVITY))
                    .filter(o -> dynamoNodes.get(0).getId().equals(getFieldByKey(o, "input").getRefValue()))
                    .filter(o -> s3Outputs.get(0).getId().equals(getFieldByKey(o, "output").getRefValue()))
                    .collect(Collectors.toList());
            assertEquals(1, activities.size());
        }

        assertEquals(1, objects.stream().filter(o->isOfType(o, "EmrCluster")).count());
        assertEquals(1, objects.stream().filter(o->isOfType(o, "Schedule")).count());
    }

    private boolean isOfType(PipelineObject object, String type) {
        Field t = getFieldByKey(object, "type");
        return t != null && type.equals(t.getStringValue());
    }

    @Test
    public void testGetRunDailySchedule() {
        PipelineObject schedule = DynamoDataPipelineHelper.getRunDailySchedule(new LocalTime(1, 0),
                DateTimeZone.forID("America/Los_Angeles")
        );

        assertStringValueEquals("Schedule", schedule, "type");
        assertStringValueEquals("1 day", schedule, "period");

        // make sure there is no zone, or Data Pipeline will not accept it
        Pattern isoDateNoZone = Pattern.compile("\\d{4}-[01]\\d-[0-3]\\dT[0-2]\\d:[0-5]\\d:[0-5]\\d");
        Matcher matcher = isoDateNoZone.matcher(getFieldByKey(schedule, "startDateTime").getStringValue());
        assertTrue(matcher.matches());
    }

    @Test
    public void testGetEMRCluster() {
        PipelineObject cluster = getEMRCluster(Region.getRegion(Regions.US_EAST_1));

        assertStringValueEquals("EmrCluster", cluster, "type");
    }

    @Test
    public void testGetDDBSourceTable() {
        String tableName = "TableX";
        String throughput = "0.1";

        PipelineObject ddbSourceTable = getDDBSourceTable(tableName, throughput);

        assertStringValueEquals("DynamoDBDataNode", ddbSourceTable, "type");
        assertStringValueEquals(tableName, ddbSourceTable, "tableName");
        assertStringValueEquals(throughput, ddbSourceTable, "readThroughputPercent");
    }

    @Test
    public void testGetDefault() throws Exception {
        String bucket = "bucket";
        PipelineObject def = DynamoDataPipelineHelper.getDefault(bucket);

        assertEquals("Default", def.getId());
        assertEquals("Default", def.getName());
    }

    @Test
    public void testGetS3BackupLocation() {
        String s3Bucket = "s3BucketX";
        Region region = Region.getRegion(Regions.DEFAULT_REGION);
        String tableName = "tableX";

        PipelineObject s3BackupLocation = getS3BackupLocation(s3Bucket, region, tableName);

        assertStringValueEquals("S3DataNode", s3BackupLocation, "type");

        String s3Path = getFieldByKey(s3BackupLocation, "directoryPath").getStringValue();

        assertTrue(s3Path.startsWith("s3://" + s3Bucket + "/"));
        assertTrue(s3Path.contains(region.getName()));
        assertTrue(s3Path.contains(tableName));
    }

    @Test
    public void getEMRActivity() {
        String tableName = "TableX";
        Region region = Region.getRegion(Regions.DEFAULT_REGION);

        String clusterId = "clusterX";
        String bucket = "bucketX";

        PipelineObject cluster = new PipelineObject().withId(clusterId);
        List<PipelineObject> objects = DynamoDataPipelineHelper.getHadoopActivity(tableName, cluster, region, bucket);

        assertEquals(3, objects.size());

        PipelineObject s3Output = null;
        PipelineObject dynamoInput = null;
        PipelineObject hadoopActivity = null;

        for (PipelineObject object : objects) {
            String type = getFieldByKey(object, "type").getStringValue();

            switch (type) {
                case DynamoDataPipelineHelper.PipelineObjectType.HADOOP_ACTIVITY:
                    hadoopActivity = object;
                    break;
                case DynamoDataPipelineHelper.PipelineObjectType.S3_DATA_NODE:
                    s3Output = object;
                    break;
                case DynamoDataPipelineHelper.PipelineObjectType.DYNAMO_DATA_NODE:
                    dynamoInput = object;
                    break;
            }
        }

        assertNotNull(dynamoInput);
        assertNotNull(hadoopActivity);
        assertNotNull(s3Output);

        assertEquals(dynamoInput.getId(), getFieldByKey(hadoopActivity, "input").getRefValue());
        assertEquals(s3Output.getId(), getFieldByKey(hadoopActivity, "output").getRefValue());
        assertEquals(clusterId, getFieldByKey(hadoopActivity, "runsOn").getRefValue());
    }

    private void assertStringValueEquals(String expected, PipelineObject object, String key) {
        assertEquals(expected, getFieldByKey(object, key).getStringValue());
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
