package org.cassandramq.engine;

import lombok.extern.slf4j.Slf4j;
import org.cassandramq.config.QueueProperties;
import org.cassandramq.metrics.QueueMetrics;
import org.cassandramq.store.ShardOwnershipStore;
import org.cassandramq.util.NamedThreadFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class ShardOwnershipManager implements OwnedShardProvider {
    private final QueueProperties properties;
    private final ShardOwnershipStore repository;
    private final QueueMetrics metrics;
    private final Set<Integer> ownedShards = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, new NamedThreadFactory("mq-lease"));
    private volatile boolean running;

    public ShardOwnershipManager(QueueProperties properties, ShardOwnershipStore repository, QueueMetrics metrics) {
        this.properties = properties;
        this.repository = repository;
        this.metrics = metrics;
    }

    public void start() {
        running = true;
        metrics.setOwnedShards(ownedShards.size());
        scheduler.scheduleWithFixedDelay(this::runAcquireOrRebalanceCycle,
                0,
                properties.queue().shardRebalanceInterval().toMillis(),
                TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::runRenewCycle,
                properties.queue().shardLeaseRenewInterval().toMillis(),
                properties.queue().shardLeaseRenewInterval().toMillis(),
                TimeUnit.MILLISECONDS);
    }

    public void stop() {
        running = false;
        for (int shardId : ownedShards) {
            repository.release(properties.queue().defaultQueue(), shardId, properties.queue().workerId());
        }
        ownedShards.clear();
        metrics.setOwnedShards(0);
        scheduler.shutdownNow();
    }

    @Override
    public Set<Integer> ownedShardsSnapshot() {
        return Collections.unmodifiableSet(ownedShards);
    }

    void runAcquireOrRebalanceCycle() {
        boolean previous = running;
        running = true;
        try {
            acquireOrRebalance();
        } finally {
            running = previous;
        }
    }

    void runRenewCycle() {
        boolean previous = running;
        running = true;
        try {
            renewOwned();
        } finally {
            running = previous;
        }
    }

    private void acquireOrRebalance() {
        if (!running) {
            return;
        }
        int desiredOwned = desiredOwnedShards();
        if (ownedShards.size() > desiredOwned) {
            releaseSurplus(ownedShards.size() - desiredOwned);
            return;
        }
        if (ownedShards.size() >= desiredOwned) {
            return;
        }

        String queue = properties.queue().defaultQueue();
        String worker = properties.queue().workerId();
        Instant expiry = Instant.now().plus(properties.queue().shardLeaseDuration());
        int total = properties.queue().totalShards();
        int startShard = Math.floorMod(worker.hashCode(), total);

        for (int i = 0; i < total && ownedShards.size() < desiredOwned; i++) {
            int shard = (startShard + i) % total;
            if (ownedShards.contains(shard)) {
                continue;
            }
            if (repository.tryAcquire(queue, shard, worker, expiry)) {
                ownedShards.add(shard);
                metrics.setOwnedShards(ownedShards.size());
                log.debug("Acquired shard {}", shard);
            }
        }
    }

    private int desiredOwnedShards() {
        int configuredCap = Math.min(properties.queue().maxOwnedShards(), properties.queue().targetOwnedShards());
        int activeWorkers = repository.countActiveWorkers(properties.queue().defaultQueue(), Instant.now());
        int effectiveWorkers = Math.max(activeWorkers, 1);
        int fairShare = Math.max(1, (int) Math.ceil((double) properties.queue().totalShards() / effectiveWorkers));
        return Math.min(configuredCap, fairShare);
    }

    private void renewOwned() {
        if (!running) {
            return;
        }
        String queue = properties.queue().defaultQueue();
        String worker = properties.queue().workerId();
        Instant expiry = Instant.now().plus(properties.queue().shardLeaseDuration());

        for (Integer shardId : ownedShards) {
            if (!repository.renew(queue, shardId, worker, expiry)) {
                ownedShards.remove(shardId);
                metrics.setOwnedShards(ownedShards.size());
                log.debug("Lost shard lease {}", shardId);
            }
        }
    }

    private void releaseSurplus(int surplusCount) {
        String queue = properties.queue().defaultQueue();
        String worker = properties.queue().workerId();
        ownedShards.stream()
                .sorted((a, b) -> Integer.compare(b, a))
                .limit(surplusCount)
                .forEach(shardId -> {
                    if (repository.release(queue, shardId, worker)) {
                        ownedShards.remove(shardId);
                        metrics.setOwnedShards(ownedShards.size());
                        log.debug("Released shard {} due to max-owned cap", shardId);
                    }
                });
    }
}
