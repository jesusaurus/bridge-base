package org.sagebionetworks.bridge.sqs;

// TODO doc
public interface PollSqsCallback {
    void callback(String messageBody) throws Exception;
}
