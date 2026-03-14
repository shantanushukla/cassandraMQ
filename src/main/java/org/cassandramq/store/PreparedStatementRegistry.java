package org.cassandramq.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import lombok.Getter;
import lombok.experimental.Accessors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;

@Getter
@Accessors(fluent = true)
public final class PreparedStatementRegistry {
    private final PreparedStatement insertMessage;
    private final PreparedStatement pollReadyMessages;
    private final PreparedStatement claimMessage;
    private final PreparedStatement completeMessage;
    private final PreparedStatement requeueRunningMessage;
    private final PreparedStatement scheduleRetry;
    private final PreparedStatement acquireShardLease;
    private final PreparedStatement acquireShardLeaseIfAbsent;
    private final PreparedStatement renewShardLease;
    private final PreparedStatement releaseShardLease;
    private final PreparedStatement selectShardOwnershipByQueue;

    public PreparedStatementRegistry(CqlSession session) {
        this.insertMessage = session.prepare(
                insertInto("queue_messages_by_shard")
                        .value("queue_name", bindMarker())
                        .value("shard_id", bindMarker())
                        .value("bucket_time", bindMarker())
                        .value("message_id", bindMarker())
                        .value("payload", bindMarker())
                        .value("status", bindMarker())
                        .value("lease_expiry", bindMarker())
                        .value("worker_id", bindMarker())
                        .value("retry_count", bindMarker())
                        .value("created_time", bindMarker())
                        .build());

        this.pollReadyMessages = session.prepare(
                selectFrom("queue_messages_by_shard")
                        .columns("queue_name", "shard_id", "bucket_time", "message_id", "payload",
                                "status", "lease_expiry", "worker_id", "retry_count", "created_time")
                        .whereColumn("queue_name").isEqualTo(bindMarker())
                        .whereColumn("shard_id").isEqualTo(bindMarker())
                        .whereColumn("bucket_time").isEqualTo(bindMarker())
                        .limit(bindMarker())
                        .build());

        this.claimMessage = session.prepare(
                update("queue_messages_by_shard")
                        .setColumn("status", bindMarker())
                        .setColumn("worker_id", bindMarker())
                        .setColumn("lease_expiry", bindMarker())
                        .whereColumn("queue_name").isEqualTo(bindMarker())
                        .whereColumn("shard_id").isEqualTo(bindMarker())
                        .whereColumn("bucket_time").isEqualTo(bindMarker())
                        .whereColumn("message_id").isEqualTo(bindMarker())
                        .ifColumn("status").isEqualTo(bindMarker())
                        .build());

        this.completeMessage = session.prepare(
                update("queue_messages_by_shard")
                        .setColumn("status", bindMarker())
                        .setColumn("worker_id", bindMarker())
                        .setColumn("lease_expiry", bindMarker())
                        .whereColumn("queue_name").isEqualTo(bindMarker())
                        .whereColumn("shard_id").isEqualTo(bindMarker())
                        .whereColumn("bucket_time").isEqualTo(bindMarker())
                        .whereColumn("message_id").isEqualTo(bindMarker())
                        .build());

        this.requeueRunningMessage = session.prepare(
                update("queue_messages_by_shard")
                        .setColumn("status", bindMarker())
                        .setColumn("worker_id", bindMarker())
                        .setColumn("lease_expiry", bindMarker())
                        .whereColumn("queue_name").isEqualTo(bindMarker())
                        .whereColumn("shard_id").isEqualTo(bindMarker())
                        .whereColumn("bucket_time").isEqualTo(bindMarker())
                        .whereColumn("message_id").isEqualTo(bindMarker())
                        .ifColumn("status").isEqualTo(bindMarker())
                        .build());

        this.scheduleRetry = session.prepare(
                insertInto("queue_messages_by_shard")
                        .value("queue_name", bindMarker())
                        .value("shard_id", bindMarker())
                        .value("bucket_time", bindMarker())
                        .value("message_id", bindMarker())
                        .value("payload", bindMarker())
                        .value("status", bindMarker())
                        .value("lease_expiry", bindMarker())
                        .value("worker_id", bindMarker())
                        .value("retry_count", bindMarker())
                        .value("created_time", bindMarker())
                        .build());

        this.acquireShardLease = session.prepare(
                update("shard_ownership")
                        .setColumn("worker_id", bindMarker())
                        .setColumn("lease_expiry", bindMarker())
                        .whereColumn("queue_name").isEqualTo(bindMarker())
                        .whereColumn("shard_id").isEqualTo(bindMarker())
                        .ifRaw("lease_expiry < toTimestamp(now())")
                        .build());

        this.acquireShardLeaseIfAbsent = session.prepare(
                insertInto("shard_ownership")
                        .value("queue_name", bindMarker())
                        .value("shard_id", bindMarker())
                        .value("worker_id", bindMarker())
                        .value("lease_expiry", bindMarker())
                        .ifNotExists()
                        .build());

        this.renewShardLease = session.prepare(
                update("shard_ownership")
                        .setColumn("lease_expiry", bindMarker())
                        .whereColumn("queue_name").isEqualTo(bindMarker())
                        .whereColumn("shard_id").isEqualTo(bindMarker())
                        .ifColumn("worker_id").isEqualTo(bindMarker())
                        .build());

        this.releaseShardLease = session.prepare(
                deleteFrom("shard_ownership")
                        .whereColumn("queue_name").isEqualTo(bindMarker())
                        .whereColumn("shard_id").isEqualTo(bindMarker())
                        .ifColumn("worker_id").isEqualTo(bindMarker())
                        .build());

        this.selectShardOwnershipByQueue = session.prepare(
                selectFrom("shard_ownership")
                        .columns("worker_id", "lease_expiry")
                        .whereColumn("queue_name").isEqualTo(bindMarker())
                        .build());
    }
}
