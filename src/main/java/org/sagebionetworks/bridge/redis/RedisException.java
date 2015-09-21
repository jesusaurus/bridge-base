package org.sagebionetworks.bridge.redis;

@SuppressWarnings("serial")
public class RedisException extends RuntimeException {

    public RedisException(final String message) {
        super(message);
    }
}
