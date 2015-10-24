package org.sagebionetworks.bridge.sqs;

import com.amazonaws.services.sqs.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO doc
public class PollSqsWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(PollSqsWorker.class);

    private PollSqsCallback callback;
    private String queueUrl;
    private int sleepTimeMillis;
    private SqsHelper sqsHelper;

    /** Callback to call when the poll worke recieves a message. */
    public final void setCallback(PollSqsCallback callback) {
        this.callback = callback;
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
                callback.callback(sqsMessage.getBody());
            } catch (Exception ex) {
                LOG.error("PollSqsWorker exception: " + ex.getMessage(), ex);
            } catch (Error err) {
                LOG.error("PollSqsWorker critical error: " + err.getMessage(), err);
            }
        }
    }

    // This is called by PollSqsWorker for every loop iteration to determine if worker should keep running. This
    // is a member method to enable mocking and is package-scoped to make it available to unit tests.
    boolean shouldKeepRunning() {
        return true;
    }
}
