package org.sagebionetworks.bridge.sqs;

/**
 * Exception to signal to the PollSqsWorker that this request deterministically fails and should not be retried. (More
 * specifically, we should suppress the exception and still delete the SQS message.
 */
@SuppressWarnings("serial")
public class PollSqsWorkerBadRequestException extends Exception {
    public PollSqsWorkerBadRequestException() {
    }

    public PollSqsWorkerBadRequestException(String message) {
        super(message);
    }

    public PollSqsWorkerBadRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    public PollSqsWorkerBadRequestException(Throwable cause) {
        super(cause);
    }
}
