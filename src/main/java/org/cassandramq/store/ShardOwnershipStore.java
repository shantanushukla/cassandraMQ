package org.cassandramq.store;

import java.time.Instant;

public interface ShardOwnershipStore {
    boolean tryAcquire(String queueName, int shardId, String workerId, Instant leaseExpiry);

    boolean renew(String queueName, int shardId, String workerId, Instant leaseExpiry);

    boolean release(String queueName, int shardId, String workerId);

    int countActiveWorkers(String queueName, Instant now);
}
