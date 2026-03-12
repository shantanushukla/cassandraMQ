package org.cassandramq.engine;

import org.cassandramq.api.MessageHandler;
import org.cassandramq.config.QueueProperties;
import org.cassandramq.metrics.QueueMetrics;
import org.cassandramq.model.Message;
import org.cassandramq.store.QueueMessageStore;

import java.time.Instant;

public final class TaskClaimEngine implements ClaimDispatcher {
    private final QueueProperties properties;
    private final QueueMessageStore repository;
    private final ExecutionPool executionPool;
    private final QueueMetrics metrics;

    public TaskClaimEngine(
            QueueProperties properties,
            QueueMessageStore repository,
            ExecutionPool executionPool,
            QueueMetrics metrics
    ) {
        this.properties = properties;
        this.repository = repository;
        this.executionPool = executionPool;
        this.metrics = metrics;
    }

    @Override
    public void tryClaimAndExecute(Message message, MessageHandler handler) {
        Instant leaseExpiry = Instant.now().plus(properties.claim().taskLeaseDuration());
        boolean claimed = repository.tryClaim(message, properties.queue().workerId(), leaseExpiry);
        if (!claimed) {
            metrics.incrementClaimFailures();
            return;
        }
        metrics.incrementClaimed();
        executionPool.execute(message, handler);
    }
}
