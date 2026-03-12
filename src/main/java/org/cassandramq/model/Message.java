package org.cassandramq.model;

import java.time.Instant;
import java.util.UUID;

public record Message(
        UUID messageId,
        String queueName,
        int shardId,
        Instant bucketTime,
        byte[] payload,
        MessageStatus status,
        Instant leaseExpiry,
        String workerId,
        int retryCount,
        Instant createdTime
) {
}
