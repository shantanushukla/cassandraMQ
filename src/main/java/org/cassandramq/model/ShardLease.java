package org.cassandramq.model;

import java.time.Instant;

public record ShardLease(int shardId, String workerId, Instant leaseExpiry) {
}
