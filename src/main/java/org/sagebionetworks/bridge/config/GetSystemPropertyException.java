package org.sagebionetworks.bridge.config;

@SuppressWarnings("serial")
public class GetSystemPropertyException extends RuntimeException {

    public GetSystemPropertyException(final String propertyName) {
        super("Cannot read system property '" + propertyName + "'.");
    }

    public GetSystemPropertyException(final String propertyName, final Throwable cause) {
        super("Cannot read system property '" + propertyName + "'.", cause);
    }
}
