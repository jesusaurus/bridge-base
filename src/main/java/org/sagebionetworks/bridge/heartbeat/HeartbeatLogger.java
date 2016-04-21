package org.sagebionetworks.bridge.heartbeat;

import java.util.concurrent.TimeUnit;

import com.jcabi.aspects.ScheduleWithFixedDelay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Heartbeat logger. Used to write something to the logs every 30 minutes to ensure you have logs for every hour.
 * Generally useful for logging utilities that assume there will be a log message every hour. Uses Jcabi.
 *
 * 30 minutes chosen to ensure there's at least 1 log (generally 2), while 60 minutes might cause a log at 1:59 and
 * 3:01 if there's severe clock skew.
 */
@ScheduleWithFixedDelay(delay = 30, unit = TimeUnit.MINUTES)
public class HeartbeatLogger implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatLogger.class);

    @Override
    public void run() {
        LOG.info("Logging heartbeat...");
    }
}
