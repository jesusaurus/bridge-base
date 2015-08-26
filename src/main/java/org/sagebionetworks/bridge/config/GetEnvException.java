package org.sagebionetworks.bridge.config;

@SuppressWarnings("serial")
public class GetEnvException extends RuntimeException {

    public GetEnvException(final String envName) {
        super("Cannot read environment variable '" + envName + "'.");
    }

    public GetEnvException(final String envName, final Throwable cause) {
        super("Cannot read environment variable '" + envName + "'.", cause);
    }
}
