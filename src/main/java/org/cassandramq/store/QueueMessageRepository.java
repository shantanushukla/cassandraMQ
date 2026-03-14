package org.cassandramq.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.cassandramq.model.Message;
import org.cassandramq.model.MessageStatus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

public final class QueueMessageRepository implements QueueMessageStore {
    private final CqlSession session;
    private final PreparedStatementRegistry statements;

    public QueueMessageRepository(CqlSession session, PreparedStatementRegistry statements) {
        this.session = session;
        this.statements = statements;
    }

    @Override
    public void insert(Message message) {
        BoundStatement bs = statements.insertMessage().bind(
                message.queueName(),
                message.shardId(),
                message.bucketTime(),
                message.messageId(),
                ByteBuffer.wrap(message.payload()),
                message.status().name(),
                message.leaseExpiry(),
                message.workerId(),
                message.retryCount(),
                message.createdTime()
        );
        session.execute(bs);
    }

    @Override
    public List<Message> pollReady(String queueName, int shardId, Instant bucketTime, int limit) {
        BoundStatement bs = statements.pollReadyMessages().bind(queueName, shardId, bucketTime, limit);
        ResultSet rs = session.execute(bs);
        List<Message> out = new ArrayList<>();
        for (Row row : rs) {
            MessageStatus status = MessageStatus.valueOf(row.getString("status"));
            if (status == MessageStatus.READY || status == MessageStatus.RETRY) {
                out.add(toMessage(row));
            }
        }
        return out;
    }

    @Override
    public List<Message> pollBucket(String queueName, int shardId, Instant bucketTime, int limit) {
        BoundStatement bs = statements.pollReadyMessages().bind(queueName, shardId, bucketTime, limit);
        ResultSet rs = session.execute(bs);
        List<Message> out = new ArrayList<>();
        for (Row row : rs) {
            out.add(toMessage(row));
        }
        return out;
    }

    @Override
    public boolean tryClaim(Message message, String workerId, Instant leaseExpiry) {
        BoundStatement bs = statements.claimMessage().bind(
                MessageStatus.RUNNING.name(),
                workerId,
                leaseExpiry,
                message.queueName(),
                message.shardId(),
                message.bucketTime(),
                message.messageId(),
                message.status().name()
        );
        Row row = session.execute(bs).one();
        return row != null && row.getBoolean("[applied]");
    }

    @Override
    public void markCompleted(Message message, String workerId) {
        markStatus(message, MessageStatus.COMPLETED, workerId, Instant.now());
    }

    @Override
    public void markFailed(Message message, String workerId) {
        markStatus(message, MessageStatus.FAILED, workerId, Instant.now());
    }

    @Override
    public void scheduleRetry(Message message, Instant retryBucket, int retryCount) {
        BoundStatement bs = statements.scheduleRetry().bind(
                message.queueName(),
                message.shardId(),
                retryBucket,
                message.messageId(),
                ByteBuffer.wrap(message.payload()),
                MessageStatus.RETRY.name(),
                null,
                null,
                retryCount,
                message.createdTime()
        );
        session.execute(bs);
    }

    @Override
    public boolean requeueExpiredRunning(Message message) {
        BoundStatement bs = statements.requeueRunningMessage().bind(
                MessageStatus.RETRY.name(),
                null,
                null,
                message.queueName(),
                message.shardId(),
                message.bucketTime(),
                message.messageId(),
                MessageStatus.RUNNING.name()
        );
        Row row = session.execute(bs).one();
        return row != null && row.getBoolean("[applied]");
    }

    private void markStatus(Message message, MessageStatus status, String workerId, Instant leaseExpiry) {
        BoundStatement bs = statements.completeMessage().bind(
                status.name(),
                workerId,
                leaseExpiry,
                message.queueName(),
                message.shardId(),
                message.bucketTime(),
                message.messageId()
        );
        session.execute(bs);
    }

    private Message toMessage(Row row) {
        ByteBuffer payloadBuffer = row.getByteBuffer("payload");
        byte[] payload = new byte[payloadBuffer.remaining()];
        payloadBuffer.get(payload);
        return new Message(
                row.getUuid("message_id"),
                row.getString("queue_name"),
                row.getInt("shard_id"),
                row.getInstant("bucket_time"),
                payload,
                MessageStatus.valueOf(row.getString("status")),
                row.getInstant("lease_expiry"),
                row.getString("worker_id"),
                row.getInt("retry_count"),
                row.getInstant("created_time")
        );
    }
}
