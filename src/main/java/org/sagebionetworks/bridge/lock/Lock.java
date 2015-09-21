package org.sagebionetworks.bridge.lock;

public interface Lock {

    /**
     * Tries to acquire a lock for the specified key. Returns a unique identifier for the lock
     * upon successfully acquiring the lock. Otherwise, throws LockNotAvailableException.
     *
     * @param key              The key to lock on.
     * @param expireInSeconds  The lock will expire after the specified amount of time.
     * @return The unique identifier for the lock which will be needed for releasing the lock.
     */
    String acquireLock(String key, int expireInSeconds);

    /**
     * Releases the lock held for the specified key. The unique lock identifier
     * must be supplied to the release the lock.
     * 
     * @param key    The key to release the lock.
     * @param lock   The unique identifier for the lock.
     * @return True, if the lock has been released; false, if the lock ID does not match. 
     */
    boolean releaseLock(String key, String lock);
}
