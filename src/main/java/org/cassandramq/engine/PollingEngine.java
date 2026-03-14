package org.cassandramq.engine;

import org.cassandramq.api.MessageHandler;
import org.cassandramq.config.QueueProperties;
import org.cassandramq.metrics.QueueMetrics;
import org.cassandramq.model.Message;
import org.cassandramq.store.QueueMessageStore;
import org.cassandramq.util.BucketTimeUtil;
import org.cassandramq.util.NamedThreadFactory;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class PollingEngine {
    private final QueueProperties properties;
    private final OwnedShardProvider ownershipManager;
    private final QueueMessageStore repository;
    private final ClaimDispatcher claimEngine;
    private final QueueMetrics metrics;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("mq-poll"));
    private final ScheduledExecutorService lagScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("mq-lag"));

    public PollingEngine(
            QueueProperties properties,
            OwnedShardProvider ownershipManager,
            QueueMessageStore repository,
            ClaimDispatcher claimEngine,
            QueueMetrics metrics
    ) {
        this.properties = properties;
        this.ownershipManager = ownershipManager;
        this.repository = repository;
        this.claimEngine = claimEngine;
        this.metrics = metrics;
    }

    public void start(MessageHandler handler) {
        scheduler.scheduleWithFixedDelay(
                () -> runCycle(handler),
                0,
                properties.poll().interval().toMillis(),
                TimeUnit.MILLISECONDS
        );
        lagScheduler.scheduleWithFixedDelay(
                this::runLagScanCycle,
                properties.metrics().lagScanInterval().toMillis(),
                properties.metrics().lagScanInterval().toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    public void stop() {
        scheduler.shutdownNow();
        lagScheduler.shutdownNow();
    }

    void runCycle(MessageHandler handler) {
        long started = System.nanoTime();
        try {
            metrics.incrementPollCycles();
            int threshold = (properties.execution().maxInflightTasks() * properties.poll().pauseInflightThresholdPercent()) / 100;
            if (metrics.inflightTasks() >= threshold) {
                return;
            }
            int claimBudget = properties.claim().maxClaimAttemptsPerCycle();
            Set<Integer> owned = ownershipManager.ownedShardsSnapshot();
            if (owned.isEmpty()) {
                return;
            }

            String queue = properties.queue().defaultQueue();
            Instant now = Instant.now();
            for (int shardId : owned) {
                for (int offset = 0; offset < properties.poll().maxBucketsPerShard(); offset++) {
                    Instant bucket = BucketTimeUtil.bucketStart(
                            now.minusSeconds((long) offset * properties.queue().bucketSizeSeconds()),
                            properties.queue().bucketSizeSeconds()
                    );

                    List<Message> candidates = repository.pollReady(queue, shardId, bucket, properties.poll().batchSize());
                    for (Message message : candidates) {
                        if (claimBudget <= 0) {
                            return;
                        }
                        metrics.incrementPolledMessages();
                        claimEngine.tryClaimAndExecute(message, handler);
                        claimBudget--;
                    }
                }
            }
        } finally {
            metrics.recordPollLatency(java.time.Duration.ofNanos(System.nanoTime() - started));
        }
    }

    void runLagScanCycle() {
        Set<Integer> owned = ownershipManager.ownedShardsSnapshot();
        if (owned.isEmpty()) {
            metrics.setQueueLagMillis(0);
            return;
        }

        String queue = properties.queue().defaultQueue();
        Instant now = Instant.now();
        long maxLagMillis = 0;
        for (int shardId : owned) {
            for (int offset = 0; offset < properties.poll().maxBucketsPerShard(); offset++) {
                Instant bucket = BucketTimeUtil.bucketStart(
                        now.minusSeconds((long) offset * properties.queue().bucketSizeSeconds()),
                        properties.queue().bucketSizeSeconds()
                );
                List<Message> candidates = repository.pollReady(queue, shardId, bucket, properties.poll().batchSize());
                for (Message message : candidates) {
                    long lagMillis = Math.max(0, now.toEpochMilli() - message.createdTime().toEpochMilli());
                    maxLagMillis = Math.max(maxLagMillis, lagMillis);
                }
            }
        }
        metrics.setQueueLagMillis(maxLagMillis);
    }
}
