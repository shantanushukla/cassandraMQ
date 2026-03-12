package org.cassandramq.engine;

import lombok.extern.slf4j.Slf4j;
import org.cassandramq.config.QueueProperties;
import org.cassandramq.metrics.QueueMetrics;
import org.cassandramq.model.Message;
import org.cassandramq.model.MessageStatus;
import org.cassandramq.store.QueueMessageStore;
import org.cassandramq.util.BucketTimeUtil;
import org.cassandramq.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class LeaseRecoveryEngine {
    private final QueueProperties properties;
    private final OwnedShardProvider ownershipManager;
    private final QueueMessageStore repository;
    private final QueueMetrics metrics;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("mq-recovery"));

    public LeaseRecoveryEngine(
            QueueProperties properties,
            OwnedShardProvider ownershipManager,
            QueueMessageStore repository,
            QueueMetrics metrics
    ) {
        this.properties = properties;
        this.ownershipManager = ownershipManager;
        this.repository = repository;
        this.metrics = metrics;
    }

    public void start() {
        if (!properties.runtime().enableLeaseRecovery()) {
            return;
        }
        scheduler.scheduleWithFixedDelay(
                this::runRecoveryCycle,
                properties.runtime().healthCheckInterval().toMillis(),
                properties.runtime().healthCheckInterval().toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    void runRecoveryCycle() {
        Set<Integer> ownedShards = ownershipManager.ownedShardsSnapshot();
        if (ownedShards.isEmpty()) {
            return;
        }

        String queueName = properties.queue().defaultQueue();
        Instant now = Instant.now();
        int scanLimit = properties.poll().batchSize();
        for (int shardId : ownedShards) {
            for (int offset = 0; offset < properties.runtime().recoveryLookbackBuckets(); offset++) {
                Instant bucket = BucketTimeUtil.bucketStart(
                        now.minusSeconds((long) offset * properties.queue().bucketSizeSeconds()),
                        properties.queue().bucketSizeSeconds()
                );
                List<Message> candidates = repository.pollBucket(queueName, shardId, bucket, scanLimit);
                for (Message message : candidates) {
                    if (message.status() != MessageStatus.RUNNING || message.leaseExpiry() == null) {
                        continue;
                    }
                    if (message.leaseExpiry().isBefore(now) && repository.requeueExpiredRunning(message)) {
                        metrics.incrementRecovered();
                        log.debug("Recovered expired RUNNING lease for message {}", message.messageId());
                    }
                }
            }
        }
    }
}
