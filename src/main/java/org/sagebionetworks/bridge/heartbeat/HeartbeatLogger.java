package org.sagebionetworks.bridge.heartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Heartbeat logger. Used to write something to the logs on a regular interval to ensure you have logs for every hour.
 * Generally useful for logging utilities that assume there will be a log message every hour.
 */
public class HeartbeatLogger implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatLogger.class);

    private int intervalMillis;

    /** Sets the heartbeat interval in minutes. */
    public final void setIntervalMinutes(int intervalMinutes) {
        // convert this internally to milliseconds
        this.intervalMillis = intervalMinutes * 60000;
    }

    /**
     * Runs the heartbeat logger. This is an infinite loop that waits the interval and logs the heartbeat message.
     * First heartbeat message happens immediately.
     */
    @Override
    public void run() {
        while (shouldKeepRunning()) {
            logHeartbeat();

            if (intervalMillis > 0) {
                try {
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException ex) {
                    LOG.warn("Interrupted while sleeping: " + ex.getMessage(), ex);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /** Helper method, used for tests to verify logging was called. */
    void logHeartbeat() {
        LOG.info("Logging heartbeat...");
    }

    /** True if the heartbeat logger should keep running. Overridden by tests to allow tests to exit. */
    boolean shouldKeepRunning() {
        return true;
    }
}
