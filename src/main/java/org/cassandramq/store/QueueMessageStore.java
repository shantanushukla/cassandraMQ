package org.cassandramq.store;

import org.cassandramq.model.Message;

import java.time.Instant;
import java.util.List;

public interface QueueMessageStore {
    void insert(Message message);

    List<Message> pollReady(String queueName, int shardId, Instant bucketTime, int limit);

    List<Message> pollBucket(String queueName, int shardId, Instant bucketTime, int limit);

    boolean tryClaim(Message message, String workerId, Instant leaseExpiry);

    void markCompleted(Message message, String workerId);

    void markFailed(Message message, String workerId);

    void scheduleRetry(Message message, Instant retryBucket, int retryCount);

    boolean requeueExpiredRunning(Message message);
}
