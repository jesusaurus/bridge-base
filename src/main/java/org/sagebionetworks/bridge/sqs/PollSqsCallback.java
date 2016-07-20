package org.sagebionetworks.bridge.sqs;

/**
 * This is used with the PollSqsWorker. Users of the PollSqsWorker should implement PollSqsCallback to handle messages
 * received by the PollSqsWorker.
 */
public interface PollSqsCallback {
    /**
     * Processes the SQS message. The PollSqsWorker assumes that if this returns normally, then the SQS message was
     * successfully processed and should be deleted to prevent duplicate processing. If this throws, the PollSqsWorker
     * does not delete the message (unless it's a PollSqsWorkerBadRequestException), so the message will be
     * re-processed.
     *
     * @param messageBody
     *         the raw SQS message body
     * @throws Exception
     *         if an error in processing happens
     */
    void callback(String messageBody) throws Exception;
}
