package org.cassandramq.config;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public final class QueueProperties {
    private static final String DEFAULT_RESOURCE = "cassandramq.properties";
    private final CassandraConfig cassandra;
    private final QueueConfig queue;
    private final PollConfig poll;
    private final ClaimConfig claim;
    private final ExecutionConfig execution;
    private final RetryConfig retry;
    private final MetricsConfig metrics;
    private final RuntimeConfig runtime;

    private QueueProperties(
            CassandraConfig cassandra,
            QueueConfig queue,
            PollConfig poll,
            ClaimConfig claim,
            ExecutionConfig execution,
            RetryConfig retry,
            MetricsConfig metrics,
            RuntimeConfig runtime
    ) {
        this.cassandra = cassandra;
        this.queue = queue;
        this.poll = poll;
        this.claim = claim;
        this.execution = execution;
        this.retry = retry;
        this.metrics = metrics;
        this.runtime = runtime;
    }

    public static QueueProperties loadDefault() {
        return loadFromResource(DEFAULT_RESOURCE);
    }

    public static QueueProperties loadFromResource(String resourcePath) {
        Properties p = new Properties();
        try (InputStream in = QueueProperties.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("Resource not found: " + resourcePath);
            }
            p.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load properties from " + resourcePath, e);
        }
        return from(p);
    }

    public static QueueProperties from(Properties baseProperties) {
        Properties p = new Properties();
        p.putAll(baseProperties);

        CassandraConfig cassandra = new CassandraConfig(
                get(p, "cassandramq.cassandra.secure-connect-bundle-path", ""),
                get(p, "cassandramq.cassandra.client-id", ""),
                get(p, "cassandramq.cassandra.client-secret", ""),
                Duration.ofMillis(getInt(p, "cassandramq.cassandra.connect-timeout-ms")),
                Duration.ofMillis(getInt(p, "cassandramq.cassandra.request-timeout-ms")),
                getInt(p, "cassandramq.cassandra.max-requests-per-connection"),
                getBoolean(p, "cassandramq.cassandra.ssl-enabled")
        );

        QueueConfig queue = new QueueConfig(
                getInt(p, "cassandramq.queue.total-shards"),
                getInt(p, "cassandramq.queue.bucket-size-seconds"),
                get(p, "cassandramq.queue.default-queue"),
                get(p, "cassandramq.queue.worker-id"),
                getInt(p, "cassandramq.queue.max-owned-shards", getInt(p, "cassandramq.queue.total-shards")),
                getInt(p, "cassandramq.queue.target-owned-shards", getInt(p, "cassandramq.queue.total-shards")),
                Duration.ofSeconds(getInt(p, "cassandramq.queue.shard-lease-duration-seconds")),
                Duration.ofSeconds(getInt(p, "cassandramq.queue.shard-lease-renew-interval-seconds")),
                Duration.ofSeconds(getInt(p, "cassandramq.queue.shard-rebalance-interval-seconds"))
        );

        PollConfig poll = new PollConfig(
                Duration.ofMillis(getInt(p, "cassandramq.poll.interval-ms")),
                getInt(p, "cassandramq.poll.batch-size"),
                getInt(p, "cassandramq.poll.max-buckets-per-shard"),
                getInt(p, "cassandramq.poll.pause-inflight-threshold-percent", 90)
        );

        ClaimConfig claim = new ClaimConfig(
                Duration.ofSeconds(getInt(p, "cassandramq.claim.task-lease-duration-seconds")),
                getInt(p, "cassandramq.claim.max-claim-attempts-per-cycle")
        );

        ExecutionConfig execution = new ExecutionConfig(
                getInt(p, "cassandramq.execution.thread-pool-size"),
                getInt(p, "cassandramq.execution.max-inflight-tasks"),
                Duration.ofSeconds(getInt(p, "cassandramq.execution.shutdown-wait-seconds"))
        );

        RetryConfig retry = new RetryConfig(
                getInt(p, "cassandramq.retry.max-attempts"),
                parseDurationList(get(p, "cassandramq.retry.backoff-sequence")),
                getInt(p, "cassandramq.retry.jitter-percent")
        );

        MetricsConfig metrics = new MetricsConfig(
                getBoolean(p, "cassandramq.metrics.enabled"),
                getBoolean(p, "cassandramq.metrics.prometheus-enabled"),
                get(p, "cassandramq.metrics.namespace"),
                Duration.ofSeconds(getInt(p, "cassandramq.metrics.lag-scan-interval-seconds"))
        );

        RuntimeConfig runtime = new RuntimeConfig(
                Duration.ofSeconds(getInt(p, "cassandramq.runtime.startup-acquire-timeout-seconds")),
                Duration.ofSeconds(getInt(p, "cassandramq.runtime.health-check-interval-seconds")),
                getBoolean(p, "cassandramq.runtime.enable-lease-recovery"),
                getInt(p, "cassandramq.runtime.recovery-lookback-buckets", 6)
        );

        QueueProperties qp = new QueueProperties(cassandra, queue, poll, claim, execution, retry, metrics, runtime);
        qp.validate();
        return qp;
    }

    public void validate() {
        requirePositive(queue.totalShards(), "queue.total-shards");
        requirePositive(queue.bucketSizeSeconds(), "queue.bucket-size-seconds");
        requirePositive(queue.maxOwnedShards(), "queue.max-owned-shards");
        requirePositive(queue.targetOwnedShards(), "queue.target-owned-shards");
        if (queue.maxOwnedShards() > queue.totalShards()) {
            throw new IllegalArgumentException("queue.max-owned-shards must be <= queue.total-shards");
        }
        if (queue.targetOwnedShards() > queue.maxOwnedShards()) {
            throw new IllegalArgumentException("queue.target-owned-shards must be <= queue.max-owned-shards");
        }
        if (queue.defaultQueue().isBlank()) {
            throw new IllegalArgumentException("cassandramq.queue.default-queue is required");
        }
        requirePositive(poll.batchSize(), "poll.batch-size");
        if (poll.pauseInflightThresholdPercent() < 1 || poll.pauseInflightThresholdPercent() > 100) {
            throw new IllegalArgumentException("poll.pause-inflight-threshold-percent must be between 1 and 100");
        }
        requirePositive(execution.threadPoolSize(), "execution.thread-pool-size");
        requirePositive(retry.maxAttempts(), "retry.max-attempts");
        requirePositive(runtime.recoveryLookbackBuckets(), "runtime.recovery-lookback-buckets");
        if (retry.jitterPercent() < 0 || retry.jitterPercent() > 100) {
            throw new IllegalArgumentException("retry.jitter-percent must be between 0 and 100");
        }
    }

    private static void requirePositive(int value, String key) {
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be > 0");
        }
    }

    private static List<Duration> parseDurationList(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Duration::parse)
                .collect(Collectors.toList());
    }

    private static String get(Properties p, String key) {
        String v = get(p, key, null);
        if (v == null) {
            throw new IllegalArgumentException("Missing property: " + key);
        }
        return v;
    }

    private static String get(Properties p, String key, String defaultValue) {
        return Objects.requireNonNullElse(System.getProperty(key), p.getProperty(key, defaultValue));
    }

    private static int getInt(Properties p, String key) {
        return Integer.parseInt(get(p, key));
    }

    private static int getInt(Properties p, String key, int defaultValue) {
        return Integer.parseInt(get(p, key, String.valueOf(defaultValue)));
    }

    private static boolean getBoolean(Properties p, String key) {
        return Boolean.parseBoolean(get(p, key));
    }

    public CassandraConfig cassandra() {
        return cassandra;
    }

    public QueueConfig queue() {
        return queue;
    }

    public PollConfig poll() {
        return poll;
    }

    public ClaimConfig claim() {
        return claim;
    }

    public ExecutionConfig execution() {
        return execution;
    }

    public RetryConfig retry() {
        return retry;
    }

    public MetricsConfig metrics() {
        return metrics;
    }

    public RuntimeConfig runtime() {
        return runtime;
    }

    public record CassandraConfig(
            String secureConnectBundlePath,
            String clientId,
            String clientSecret,
            Duration connectTimeout,
            Duration requestTimeout,
            int maxRequestsPerConnection,
            boolean sslEnabled
    ) {
    }

    public record QueueConfig(
            int totalShards,
            int bucketSizeSeconds,
            String defaultQueue,
            String workerId,
            int maxOwnedShards,
            int targetOwnedShards,
            Duration shardLeaseDuration,
            Duration shardLeaseRenewInterval,
            Duration shardRebalanceInterval
    ) {
    }

    public record PollConfig(
            Duration interval,
            int batchSize,
            int maxBucketsPerShard,
            int pauseInflightThresholdPercent
    ) {
    }

    public record ClaimConfig(Duration taskLeaseDuration, int maxClaimAttemptsPerCycle) {
    }

    public record ExecutionConfig(int threadPoolSize, int maxInflightTasks, Duration shutdownWait) {
    }

    public record RetryConfig(int maxAttempts, List<Duration> backoffSequence, int jitterPercent) {
    }

    public record MetricsConfig(boolean enabled, boolean prometheusEnabled, String namespace, Duration lagScanInterval) {
    }

    public record RuntimeConfig(
            Duration startupAcquireTimeout,
            Duration healthCheckInterval,
            boolean enableLeaseRecovery,
            int recoveryLookbackBuckets
    ) {
    }
}
