package org.cassandramq.config;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueuePropertiesTest {
    @Test
    void loadsAndValidatesWithDefaults() {
        QueueProperties properties = QueueProperties.from(baseProperties());
        assertEquals("default", properties.queue().defaultQueue());
        assertEquals(512, properties.queue().totalShards());
    }

    @Test
    void failsWhenMaxOwnedShardsExceedsTotalShards() {
        Properties p = baseProperties();
        p.setProperty("cassandramq.queue.max-owned-shards", "999");

        assertThrows(IllegalArgumentException.class, () -> QueueProperties.from(p));
    }

    @Test
    void failsWhenTargetOwnedShardsExceedsMaxOwnedShards() {
        Properties p = baseProperties();
        p.setProperty("cassandramq.queue.target-owned-shards", "65");

        assertThrows(IllegalArgumentException.class, () -> QueueProperties.from(p));
    }

    @Test
    void failsWhenLagScanIntervalIsNonPositive() {
        Properties p = baseProperties();
        p.setProperty("cassandramq.metrics.lag-scan-interval-seconds", "0");

        assertThrows(IllegalArgumentException.class, () -> QueueProperties.from(p));
    }

    private Properties baseProperties() {
        Properties p = new Properties();
        p.setProperty("cassandramq.cassandra.secure-connect-bundle-path", "/tmp/secure-connect-test-cluster.zip");
        p.setProperty("cassandramq.cassandra.client-id", "client-id");
        p.setProperty("cassandramq.cassandra.client-secret", "client-secret");
        p.setProperty("cassandramq.cassandra.username", "");
        p.setProperty("cassandramq.cassandra.password", "");
        p.setProperty("cassandramq.cassandra.connect-timeout-ms", "5000");
        p.setProperty("cassandramq.cassandra.request-timeout-ms", "3000");
        p.setProperty("cassandramq.cassandra.max-requests-per-connection", "1024");
        p.setProperty("cassandramq.cassandra.ssl-enabled", "false");

        p.setProperty("cassandramq.queue.total-shards", "512");
        p.setProperty("cassandramq.queue.bucket-size-seconds", "10");
        p.setProperty("cassandramq.queue.default-queue", "default");
        p.setProperty("cassandramq.queue.worker-id", "worker-1");
        p.setProperty("cassandramq.queue.max-owned-shards", "64");
        p.setProperty("cassandramq.queue.target-owned-shards", "32");
        p.setProperty("cassandramq.queue.shard-lease-duration-seconds", "30");
        p.setProperty("cassandramq.queue.shard-lease-renew-interval-seconds", "10");
        p.setProperty("cassandramq.queue.shard-rebalance-interval-seconds", "30");

        p.setProperty("cassandramq.poll.interval-ms", "100");
        p.setProperty("cassandramq.poll.batch-size", "200");
        p.setProperty("cassandramq.poll.max-buckets-per-shard", "2");
        p.setProperty("cassandramq.poll.pause-inflight-threshold-percent", "90");

        p.setProperty("cassandramq.claim.task-lease-duration-seconds", "30");
        p.setProperty("cassandramq.claim.max-claim-attempts-per-cycle", "1000");

        p.setProperty("cassandramq.execution.thread-pool-size", "64");
        p.setProperty("cassandramq.execution.max-inflight-tasks", "10000");
        p.setProperty("cassandramq.execution.shutdown-wait-seconds", "30");

        p.setProperty("cassandramq.retry.max-attempts", "10");
        p.setProperty("cassandramq.retry.backoff-sequence", "PT1S,PT5S");
        p.setProperty("cassandramq.retry.jitter-percent", "10");

        p.setProperty("cassandramq.metrics.enabled", "true");
        p.setProperty("cassandramq.metrics.prometheus-enabled", "true");
        p.setProperty("cassandramq.metrics.namespace", "cassandramq");
        p.setProperty("cassandramq.metrics.lag-scan-interval-seconds", "15");

        p.setProperty("cassandramq.runtime.startup-acquire-timeout-seconds", "20");
        p.setProperty("cassandramq.runtime.health-check-interval-seconds", "10");
        p.setProperty("cassandramq.runtime.enable-lease-recovery", "true");
        p.setProperty("cassandramq.runtime.recovery-lookback-buckets", "6");
        return p;
    }
}
