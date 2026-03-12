package org.cassandramq.engine;

import org.junit.jupiter.api.Test;
import org.cassandramq.config.QueueProperties;
import org.cassandramq.metrics.QueueMetrics;
import org.cassandramq.store.ShardOwnershipStore;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ShardOwnershipManagerTest {
    @Test
    void rebalancesUsingActiveWorkerFairShare() {
        QueueProperties properties = EngineTestSupport.properties();
        QueueMetrics metrics = new QueueMetrics();
        InMemoryOwnershipStore store = new InMemoryOwnershipStore();
        ShardOwnershipManager manager = new ShardOwnershipManager(properties, store, metrics);

        store.activeWorkers = 1;
        manager.runAcquireOrRebalanceCycle();
        assertEquals(16, manager.ownedShardsSnapshot().size());

        store.activeWorkers = 20;
        manager.runAcquireOrRebalanceCycle();
        assertEquals(4, manager.ownedShardsSnapshot().size());
        assertEquals(4, metrics.ownedShards());
    }

    @Test
    void renewRemovesLostLeases() {
        QueueProperties properties = EngineTestSupport.properties();
        QueueMetrics metrics = new QueueMetrics();
        InMemoryOwnershipStore store = new InMemoryOwnershipStore();
        ShardOwnershipManager manager = new ShardOwnershipManager(properties, store, metrics);

        store.activeWorkers = 1;
        manager.runAcquireOrRebalanceCycle();
        int existingShard = manager.ownedShardsSnapshot().stream().findFirst().orElseThrow();
        store.shardsToFailRenew.add(existingShard);

        manager.runRenewCycle();

        assertEquals(15, manager.ownedShardsSnapshot().size());
    }

    private static final class InMemoryOwnershipStore implements ShardOwnershipStore {
        private final Set<Integer> owned = ConcurrentHashMap.newKeySet();
        private final Set<Integer> shardsToFailRenew = ConcurrentHashMap.newKeySet();
        private int activeWorkers = 1;

        @Override
        public boolean tryAcquire(String queueName, int shardId, String workerId, Instant leaseExpiry) {
            return owned.add(shardId);
        }

        @Override
        public boolean renew(String queueName, int shardId, String workerId, Instant leaseExpiry) {
            return !shardsToFailRenew.contains(shardId);
        }

        @Override
        public boolean release(String queueName, int shardId, String workerId) {
            return owned.remove(shardId);
        }

        @Override
        public int countActiveWorkers(String queueName, Instant now) {
            return activeWorkers;
        }
    }
}
