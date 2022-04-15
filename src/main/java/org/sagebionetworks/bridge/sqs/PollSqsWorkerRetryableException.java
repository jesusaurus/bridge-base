package org.sagebionetworks.bridge.sqs;

/**
 * PollSqsWorker retries on all exception other than PollSqsWorkerBadRequestException. However, some exceptions (such
 * as Synapse not being writable for weekly maintenance) are run-of-the-mill and expected. We still want to retry, but
 * we don't want to log an error. In those cases, you should use this exception.
 */
@SuppressWarnings("serial")
public class PollSqsWorkerRetryableException extends Exception {
    public PollSqsWorkerRetryableException() {
    }

    public PollSqsWorkerRetryableException(String message) {
        super(message);
    }

    public PollSqsWorkerRetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    public PollSqsWorkerRetryableException(Throwable cause) {
        super(cause);
    }
}
