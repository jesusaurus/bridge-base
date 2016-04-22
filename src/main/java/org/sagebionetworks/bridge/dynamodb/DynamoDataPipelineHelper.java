package org.sagebionetworks.bridge.dynamodb;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.amazonaws.regions.Region;
import com.amazonaws.services.datapipeline.model.Field;
import com.amazonaws.services.datapipeline.model.PipelineObject;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalTime;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Creates a Data Pipeline for backing up Dynamo DB tables within a region.
 */
public class DynamoDataPipelineHelper {
    public static final class PipelineObjectType {
        public static final String DYNAMO_DATA_NODE = "DynamoDBDataNode";
        public static final String S3_DATA_NODE = "S3DataNode";
        public static final String EMR_ACTIVITY = "EmrActivity";
        public static final String SCHEDULE = "Schedule";
        public static final String EMR_CLUSTER = "EmrCluster";
    }

    private static final String READ_THROUGHPUT_PERCENTAGE = "0.3";

    public static Optional<String> getStringValue(PipelineObject o, String key) {
        return o.getFields().stream().filter(f -> key.equals(f.getKey())).findFirst().map(Field::getStringValue);
    }

    public static Optional<String> getRefValue(PipelineObject o, String key) {
        return o.getFields().stream().filter(f -> key.equals(f.getKey())).findFirst().map(Field::getRefValue);
    }

    /**
     * Creates data pipeline objects for a daily data pipeline that will export data for DynamoDB tables in a region.
     *
     * @param tableNames   dynamoDB tables to dump
     * @param dynamoRegion region that the dynamoDB tables belong to
     * @param s3Bucket     s3 bucket for dynamo table dump and logs, table dumps will go in
     *                     s3://s3Bucket/regionName/tableName/, logs will go in s3://s3Bucket/logs
     * @param schedule     default schedule
     */
    public static List<PipelineObject> createPipelineObjects(Region dynamoRegion,
                                                             Collection<String> tableNames,
                                                             String s3Bucket,
                                                             PipelineObject schedule) {
        List<PipelineObject> pipelineObjects = Lists.newArrayList();

        pipelineObjects.add(schedule);

        pipelineObjects.add(getDefault(s3Bucket));

        PipelineObject cluster = getEMRCluster(dynamoRegion);

        pipelineObjects.add(cluster);

        for (String tableName : tableNames) {
            pipelineObjects.addAll(getEMRActivity(tableName, cluster, dynamoRegion, s3Bucket));
        }

        return pipelineObjects;
    }

    /**
     * Creates data pipeline objects for a daily data pipeline that will export data for DynamoDB tables in a region.
     *
     * @param tableNames     dynamoDB tables to dump
     * @param dynamoRegion   region that the dynamoDB tables belong to
     * @param s3Bucket       s3 bucket for dynamo table dump and logs, table dumps will go in
     *                       s3://s3Bucket/regionName/tableName/, logs will go in s3://s3Bucket/logs
     * @param localStartTime time to begin running pipeline each day
     */
    public static List<PipelineObject> createPipelineObjects(Region dynamoRegion,
                                                             Collection<String> tableNames,
                                                             String s3Bucket,
                                                             LocalTime localStartTime,
                                                             DateTimeZone localDateTimeZone) {

        return createPipelineObjects(dynamoRegion,tableNames,s3Bucket, getRunDailySchedule(localStartTime, localDateTimeZone));
    }

    static PipelineObject getRunDailySchedule(LocalTime localStartTime, DateTimeZone localTimeZone) {
        String name = "DailySchedule";
        String id = "Schedule";

        DateTime dateTime = localStartTime.toDateTimeToday(localTimeZone).toDateTime(DateTimeZone.UTC);

        // Drop to second-level precision, or Data Pipeline will reject
        String startDateTimeString = dateTime.toString(ISODateTimeFormat.dateHourMinuteSecond());

        return new PipelineObject().withName(name).withId(id).withFields(
                new Field().withKey("type").withStringValue(PipelineObjectType.SCHEDULE),
                new Field().withKey("startDateTime").withStringValue(startDateTimeString),
                new Field().withKey("period").withStringValue("1 day")
        );
    }

    /**
     * @param s3Bucket bucket for output, Data Pipeline logs will go in s3://s3Bucket/logs
     * @return special PipelineObject that other pipeline objects in the same definition will inherit from by default
     */
    static PipelineObject getDefault(String s3Bucket) {
        String name = "Default";
        String id = "Default";

        return new PipelineObject().withName(name).withId(id).withFields(
                new Field().withKey("scheduleType").withStringValue("CRON"),
                new Field().withKey("schedule").withRefValue("Schedule"),
                new Field().withKey("failureAndRerunMode").withStringValue("CASCADE"),
                new Field().withKey("resourceRole").withStringValue("DataPipelineDefaultResourceRole"),
                new Field().withKey("role").withStringValue("DataPipelineDefaultRole"),
                new Field().withKey("pipelineLogUri").withStringValue("s3://" + s3Bucket + "/logs")
        );
    }

    static PipelineObject getDDBSourceTable(String tableName, String throughputRatio) {
        String name = "DDBSourceTable-" + tableName;
        String id = "DDBSourceTable-" + tableName;


        Field type = new Field().withKey("type").withStringValue(PipelineObjectType.DYNAMO_DATA_NODE);
        Field tableNameField = new Field().withKey("tableName").withStringValue(tableName);
        Field readThroughputPercent = new Field().withKey("readThroughputPercent").withStringValue(throughputRatio);

        List<Field> fieldsList = Lists.newArrayList(tableNameField, type, readThroughputPercent);

        return new PipelineObject().withName(name).withId(id).withFields(fieldsList);
    }

    /**
     * @param s3Bucket  s3 bucket for dynamo table dump, files will go in s3://s3Bucket/regionName/tableName/
     * @param region    region of dynamo table
     * @param tableName name of dynamo table
     * @return s3 data node for output from dynamo table dump
     */
    static PipelineObject getS3BackupLocation(String s3Bucket, Region region, String tableName) {
        String name = "S3BackupLocation-" + tableName;
        String id = "S3BackupLocation-" + tableName;
        String filename = "s3://" + s3Bucket + "/" + region.getName() + "/" + tableName +
                "/#{format(@scheduledStartTime, 'YYYY-MM-dd-HH-mm-ss')}";


        Field type = new Field().withKey("type").withStringValue(PipelineObjectType.S3_DATA_NODE);
        Field directoryPath = new Field().withKey("directoryPath").withStringValue(filename);
        List<Field> fieldsList = Lists.newArrayList(type, directoryPath);

        return new PipelineObject().withName(name).withId(id).withFields(fieldsList);
    }

    static List<PipelineObject> getEMRActivity(String tableName, PipelineObject assignedCluster, Region region, String bucket) {
        String name = "TableBackupActivity-" + tableName;
        String id = "TableBackupActivity-" + tableName;

        PipelineObject inputTable = getDDBSourceTable(tableName, READ_THROUGHPUT_PERCENTAGE);

        PipelineObject outputLocation = getS3BackupLocation(bucket, region, tableName);

        String stepString = "s3://dynamodb-emr-" + region.getName() + "/emr-ddb-storage-" +
                "handler/2.1.0/emr-ddb-2.1.0.jar,org.apache.hadoop.dynamodb.tools.DynamoDbExport," +
                "#{output.directoryPath},#{input.tableName},#{input.readThroughputPercent}";

        List<Field> fieldsList = Lists.newArrayList(
                new Field().withKey("type").withStringValue(PipelineObjectType.EMR_ACTIVITY),
                new Field().withKey("input").withRefValue(inputTable.getId()),
                new Field().withKey("output").withRefValue(outputLocation.getId()),
                new Field().withKey("runsOn").withRefValue(assignedCluster.getId()),
                new Field().withKey("maximumRetries").withStringValue("2"),
                new Field().withKey("step").withStringValue(stepString)
        );

        PipelineObject activity = new PipelineObject().withName(name).withId(id).withFields(fieldsList);

        return Lists.newArrayList(inputTable, outputLocation, activity);
    }

    static PipelineObject getEMRCluster(Region region) {
        String name = "EmrClusterForBackup";
        String id = "EmrClusterForBackup";

        //Discussion on scaling: https://forums.aws.amazon.com/thread.jspa?messageID=667827

        return new PipelineObject().withName(name).withId(id).withFields(
                new Field().withKey("type").withStringValue(PipelineObjectType.EMR_CLUSTER),
                new Field().withKey("amiVersion").withStringValue("3.8.0"),
                new Field().withKey("masterInstanceType").withStringValue("m1.medium"),
                new Field().withKey("coreInstanceType").withStringValue("m1.medium"),
                new Field().withKey("coreInstanceCount").withStringValue("1"),
                new Field().withKey("region").withStringValue(region.getName()),
                new Field().withKey("terminateAfter").withStringValue("12 hours"),
                new Field().withKey("role").withStringValue("DataPipelineDefaultRole"),
                new Field().withKey("bootstrapAction").withStringValue("s3://elasticmapreduce" +
                        "/bootstrap-actions/configure-hadoop, --yarn-key-value,yarn.nodemanager.resource" +
                        ".memory-mb=11520," +
                        "--yarn-key-value,yarn.scheduler.maximum-allocation-mb=11520," +
                        "--yarn-key-value,yarn.scheduler.minimum-allocation-mb=1440," +
                        "--yarn-key-value,yarn.app.mapreduce.am.resource.mb=2880," +
                        "--mapred-key-value,mapreduce.map.memory.mb=5760," +
                        "--mapred-key-value,mapreduce.map.java.opts=-Xmx4608M," +
                        "--mapred-key-value,mapreduce.reduce.memory.mb=2880," +
                        "--mapred-key-value,mapreduce.reduce.java.opts=-Xmx2304m," +
                        "--mapred-key-value,mapreduce.map.speculative=false")
        );
    }
}
