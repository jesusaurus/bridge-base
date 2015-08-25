package org.sagebionetworks.bridge.config;

@SuppressWarnings("serial")
public class InvalidEnvironmentException extends RuntimeException {

    public InvalidEnvironmentException(final String envName) {
        super("Invalid environment '" + envName + "'.");
    }
}
