package org.sagebionetworks.bridge.dynamodb.test;

import org.sagebionetworks.bridge.dynamodb.DynamoProjection;
import org.sagebionetworks.bridge.dynamodb.DynamoThroughput;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;

@DynamoDBTable(tableName = "TestTask")
@DynamoThroughput(writeCapacity=18L, readCapacity=20L)
public class TestTask {

    private String healthCode;
    private String guid;
    private String schedulePlanGuid;
    private Long scheduledOn;
    private Long expiresOn;
    private Long startedOn;
    private Long finishedOn;

    @DynamoDBHashKey
    @DynamoDBIndexHashKey(attributeName = "healthCode", globalSecondaryIndexName = "healthCode-scheduledOn-index")
    @DynamoProjection(projectionType=ProjectionType.ALL, globalSecondaryIndexName = "healthCode-scheduledOn-index")
    public String getHealthCode() {
        return healthCode;
    }
    public void setHealthCode(String healthCode) {
        this.healthCode = healthCode;
    }
    @DynamoDBRangeKey
    @DynamoDBIndexHashKey(attributeName = "guid", globalSecondaryIndexName ="guid-index")
    @DynamoProjection(projectionType=ProjectionType.INCLUDE, globalSecondaryIndexName = "guid-index")
    public String getGuid() {
        return guid;
    }
    public void setGuid(String guid) {
        this.guid = guid;
    }
    @DynamoDBAttribute
    public String getSchedulePlanGuid() {
        return schedulePlanGuid;
    }
    public void setSchedulePlanGuid(String schedulePlanGuid) {
        this.schedulePlanGuid = schedulePlanGuid;
    }
    @DynamoDBIndexRangeKey(attributeName = "scheduledOn", globalSecondaryIndexName = "healthCode-scheduledOn-index")
    public Long getScheduledOn() {
        return scheduledOn;
    }
    public void setScheduledOn(Long scheduledOn) {
        this.scheduledOn = scheduledOn;
    }
    @DynamoDBAttribute
    public Long getExpiresOn() {
        return expiresOn;
    }
    public void setExpiresOn(Long expiresOn) {
        this.expiresOn = expiresOn;
    }
    @DynamoDBAttribute
    public Long getStartedOn() {
        return startedOn;
    }
    public void setStartedOn(Long startedOn) {
        this.startedOn = startedOn;
    }
    @DynamoDBAttribute
    public Long getFinishedOn() {
        return finishedOn;
    }
    public void setFinishedOn(Long finishedOn) {
        this.finishedOn = finishedOn;
    }
}
