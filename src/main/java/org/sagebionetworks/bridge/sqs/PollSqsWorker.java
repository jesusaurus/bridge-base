package org.sagebionetworks.bridge.sqs;

import java.util.concurrent.ExecutorService;

import com.amazonaws.services.sqs.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates the common worker logic that polls SQS for messages, passes the messages to a callback, and
 * then deletes the message. This can be configured for a single SQS queue URL with and a sleep time between each loop.
 */
public class PollSqsWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(PollSqsWorker.class);

    private PollSqsCallback callback;
    private ExecutorService executorService;
    private String queueUrl;
    private int sleepTimeMillis;
    private SqsHelper sqsHelper;

    /** Callback to call when the poll worker recieves a message. */
    public final void setCallback(PollSqsCallback callback) {
        this.callback = callback;
    }

    /**
     * <p>
     * If provided, the PollSqsWorker will use this ExecutorService to schedule tasks. This is generally used to
     * execute worker tasks in a thread pool.
     * </p>
     * <p>
     * If not provided, the PollSqsWorker will run worker tasks in single-threaded mode.
     * </p>
     */
    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    /** Queue URL to poll. */
    public final void setQueueUrl(String queueUrl) {
        this.queueUrl = queueUrl;
    }

    /** Time to sleep between each poll attempt, in milliseconds. */
    public final void setSleepTimeMillis(int sleepTimeMillis) {
        this.sleepTimeMillis = sleepTimeMillis;
    }

    /** SQS helper. Messages for regular exports and redrives are received through SQS. */
    public final void setSqsHelper(SqsHelper sqsHelper) {
        this.sqsHelper = sqsHelper;
    }

    /** Call this to kick off the worker thread. Or schedule this with an Executor. */
    @Override
    public void run() {
        while (shouldKeepRunning()) {
            // Without this sleep statement, really weird things happen when we Ctrl+C the process. (Not relevant for
            // production, but happens all the time for local testing.) Empirically, it takes up to 125ms for the JVM
            // to shut down cleanly.) Plus, it prevents us from polling the SQS queue too fast when there are a lot of
            // messages.
            if (sleepTimeMillis > 0) {
                try {
                    Thread.sleep(sleepTimeMillis);
                } catch (InterruptedException ex) {
                    LOG.warn("Interrupted while sleeping: " + ex.getMessage(), ex);
                }
            }

            try {
                // get request from SQS
                Message sqsMessage = sqsHelper.poll(queueUrl);
                if (sqsMessage == null) {
                    // No messages yet. Loop around again.
                    continue;
                }

                if (executorService != null) {
                    // Execute callback in a separate thread.
                    executorService.execute(() -> {
                        try {
                            executeCallbackForMessage(sqsMessage);
                        } catch (Exception ex) {
                            LOG.error("PollSqsWorker exception in worker thread: " + ex.getMessage(), ex);
                        } catch (Error err) {
                            LOG.error("PollSqsWorker critical error in worker thread: " + err.getMessage(), err);
                        }
                    });
                } else {
                    // Execute callback in the same thread.
                    executeCallbackForMessage(sqsMessage);
                }
            } catch (Exception ex) {
                LOG.error("PollSqsWorker exception: " + ex.getMessage(), ex);
            } catch (Error err) {
                LOG.error("PollSqsWorker critical error: " + err.getMessage(), err);
            }
        }
    }

    // Helper method that handles calling the callback and deleting the message from the queue on success.
    private void executeCallbackForMessage(Message sqsMessage) throws Exception {
        try {
            try {
                callback.callback(sqsMessage.getBody());
            } catch (PollSqsWorkerBadRequestException ex) {
                // This is a bad request. It should not be retried. Log a warning and suppress.
                LOG.warn("PollSqsWorker bad request: " + ex.getMessage(), ex);
            }

            // If the callback doesn't throw, this means it's successfully processed the message, and we should
            // delete it from SQS to prevent re-processing the message.
            sqsHelper.deleteMessage(queueUrl, sqsMessage.getReceiptHandle());
        } catch (PollSqsWorkerRetryableException ex) {
            LOG.warn("PollSqsWorker retryable exception:" + ex.getMessage(), ex);
        }
    }

    // This is called by PollSqsWorker for every loop iteration to determine if worker should keep running. This
    // is a member method to enable mocking and is package-scoped to make it available to unit tests.
    boolean shouldKeepRunning() {
        return true;
    }
}
