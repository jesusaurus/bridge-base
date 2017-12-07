package org.sagebionetworks.bridge.worker;

/**
 * Functional interface that consumes a single argument, returns no values, and can throw exceptions. This is generally
 * used by the Bridge Worker Platform, and subprojects can just implement this interface.
 */
@FunctionalInterface
public interface ThrowingConsumer<T> {
    /** Accepts the value of the given type and performs processing as needed. */
    void accept(T t) throws Exception;
}
