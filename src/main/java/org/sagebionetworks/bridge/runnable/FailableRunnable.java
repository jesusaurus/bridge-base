package org.sagebionetworks.bridge.runnable;

/**
 * <p>
 * Runnable doesn't allow checked exceptions. However, for test code, it's sometimes useful to have a "runnable" that
 * can throw any exception.
 * </p>
 * <p>
 * To make this Java 8 lambda compatible, do not add any other methods to this interface.
 * </p>
 */
@FunctionalInterface
public interface FailableRunnable {
    /** Arbitrary test code. Consumers should implement this. */
    void run() throws Exception;
}
