package org.sagebionetworks.bridge.redis;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;

import org.sagebionetworks.bridge.lock.Lock;
import org.sagebionetworks.bridge.lock.LockNotAvailableException;

public class RedisLock implements Lock {

    private final JedisOps ops;

    public RedisLock(JedisOps ops) {
        checkNotNull(ops);
        this.ops = ops;
    }

    @Override
    public String acquireLock(final String key, final int expireInSeconds) {
        checkNotNull(key);
        checkArgument(expireInSeconds > 0);
        final String lock = UUID.randomUUID().toString();
        final Long result = ops.setnx(key, lock);
        if (result != 1L) {
            // When fail to obtain the lock
            // Force to expire the key if not already set to expire
            final Long expire = ops.ttl(key);
            if (expire < 0L || expire > expireInSeconds) {
                expire(key, expireInSeconds);
            }
            throw new LockNotAvailableException(key);
        }
        expire(key, expireInSeconds);
        return lock;
    }

    @Override
    public boolean releaseLock(final String key, final String lock) {
        checkNotNull(key);
        checkNotNull(lock);
        final String lockId = ops.get(key);
        if (!lock.equals(lockId)) {
            return false;
        }
        final Long result = ops.del(key);
        if (result == 0L) {
            throw new RedisException("Lock not released for " + key + ".");
        }
        return true;
    }

    private void expire(final String key, final int expireInSeconds) {
        ops.expire(key, expireInSeconds);
    }
}
