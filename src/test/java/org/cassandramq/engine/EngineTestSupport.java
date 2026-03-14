package org.cassandramq.engine;

import org.cassandramq.config.QueueProperties;

import java.util.Properties;

final class EngineTestSupport {
    private EngineTestSupport() {
    }

    static QueueProperties properties() {
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

        p.setProperty("cassandramq.queue.total-shards", "64");
        p.setProperty("cassandramq.queue.bucket-size-seconds", "10");
        p.setProperty("cassandramq.queue.default-queue", "default");
        p.setProperty("cassandramq.queue.worker-id", "worker-1");
        p.setProperty("cassandramq.queue.max-owned-shards", "64");
        p.setProperty("cassandramq.queue.target-owned-shards", "16");
        p.setProperty("cassandramq.queue.shard-lease-duration-seconds", "30");
        p.setProperty("cassandramq.queue.shard-lease-renew-interval-seconds", "10");
        p.setProperty("cassandramq.queue.shard-rebalance-interval-seconds", "30");

        p.setProperty("cassandramq.poll.interval-ms", "100");
        p.setProperty("cassandramq.poll.batch-size", "200");
        p.setProperty("cassandramq.poll.max-buckets-per-shard", "2");
        p.setProperty("cassandramq.poll.pause-inflight-threshold-percent", "90");

        p.setProperty("cassandramq.claim.task-lease-duration-seconds", "30");
        p.setProperty("cassandramq.claim.max-claim-attempts-per-cycle", "3");

        p.setProperty("cassandramq.execution.thread-pool-size", "2");
        p.setProperty("cassandramq.execution.max-inflight-tasks", "10");
        p.setProperty("cassandramq.execution.shutdown-wait-seconds", "2");

        p.setProperty("cassandramq.retry.max-attempts", "2");
        p.setProperty("cassandramq.retry.backoff-sequence", "PT1S,PT5S");
        p.setProperty("cassandramq.retry.jitter-percent", "0");

        p.setProperty("cassandramq.metrics.enabled", "false");
        p.setProperty("cassandramq.metrics.prometheus-enabled", "false");
        p.setProperty("cassandramq.metrics.namespace", "cassandramq");
        p.setProperty("cassandramq.metrics.lag-scan-interval-seconds", "15");

        p.setProperty("cassandramq.runtime.startup-acquire-timeout-seconds", "20");
        p.setProperty("cassandramq.runtime.health-check-interval-seconds", "10");
        p.setProperty("cassandramq.runtime.enable-lease-recovery", "true");
        p.setProperty("cassandramq.runtime.recovery-lookback-buckets", "3");
        return QueueProperties.from(p);
    }
}
