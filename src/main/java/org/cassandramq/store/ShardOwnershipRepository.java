package org.cassandramq.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

public final class ShardOwnershipRepository implements ShardOwnershipStore {
    private final CqlSession session;
    private final PreparedStatementRegistry statements;

    public ShardOwnershipRepository(CqlSession session, PreparedStatementRegistry statements) {
        this.session = session;
        this.statements = statements;
    }

    @Override
    public boolean tryAcquire(String queueName, int shardId, String workerId, Instant leaseExpiry) {
        BoundStatement updateExpired = statements.acquireShardLease().bind(workerId, leaseExpiry, queueName, shardId);
        Row updated = session.execute(updateExpired).one();
        if (updated != null && updated.getBoolean("[applied]")) {
            return true;
        }

        BoundStatement insertIfAbsent = statements.acquireShardLeaseIfAbsent().bind(queueName, shardId, workerId, leaseExpiry);
        Row inserted = session.execute(insertIfAbsent).one();
        return inserted != null && inserted.getBoolean("[applied]");
    }

    @Override
    public boolean renew(String queueName, int shardId, String workerId, Instant leaseExpiry) {
        BoundStatement bs = statements.renewShardLease().bind(leaseExpiry, queueName, shardId, workerId);
        Row row = session.execute(bs).one();
        return row != null && row.getBoolean("[applied]");
    }

    @Override
    public boolean release(String queueName, int shardId, String workerId) {
        BoundStatement bs = statements.releaseShardLease().bind(queueName, shardId, workerId);
        Row row = session.execute(bs).one();
        return row != null && row.getBoolean("[applied]");
    }

    @Override
    public int countActiveWorkers(String queueName, Instant now) {
        BoundStatement bs = statements.selectShardOwnershipByQueue().bind(queueName);
        ResultSet rs = session.execute(bs);
        Set<String> workers = new HashSet<>();
        for (Row row : rs) {
            String workerId = row.getString("worker_id");
            Instant leaseExpiry = row.getInstant("lease_expiry");
            if (workerId != null && !workerId.isBlank() && leaseExpiry != null && leaseExpiry.isAfter(now)) {
                workers.add(workerId);
            }
        }
        return workers.size();
    }
}
