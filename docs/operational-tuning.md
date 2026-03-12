# Operational Tuning Guide

## Baseline targets

- Throughput target: `100k+ msg/s`
- Primary scaling knobs: shard count, worker replicas, poll batch size, execution concurrency

## Recommended initial settings

- `cassandramq.queue.total-shards=500`
- `cassandramq.queue.max-owned-shards=500`
- `cassandramq.queue.target-owned-shards=25`
- `cassandramq.poll.batch-size=200`
- `cassandramq.poll.max-buckets-per-shard=2`
- `cassandramq.claim.max-claim-attempts-per-cycle=500`
- `cassandramq.execution.thread-pool-size` matched to CPU
- `cassandramq.execution.max-inflight-tasks` at least `5x` thread pool size

## Shard ownership behavior

- Keep `target-owned-shards` near `totalShards / workerReplicas`.
- `ShardOwnershipManager` also applies dynamic fair-share using active worker leases.
- Keep shard lease renew interval significantly lower than lease duration.
  - Example: duration `30s`, renew interval `10s`.

## Polling and claim pressure

- Increase `poll.batch-size` and `max-claim-attempts-per-cycle` together.
- Use `poll.pause-inflight-threshold-percent` (default `90`) to avoid overfilling execution.
- If retries are high, inspect handler latency and downstream dependency health.

## Retry/backoff tuning

- Keep first backoff short for transient failures.
- Increase later backoffs to reduce hot-loop retries.
- Example: `PT1S,PT5S,PT30S,PT5M`.

## Cassandra guidance

- Keep partitions bounded by `(queue_name, shard_id, bucket_time)`.
- Maintain sufficient shards to distribute write/read load.
- Use LWT only for shard lease and claim CAS paths.
- Monitor read/write latency percentiles and coordinator load.

## Metrics to monitor

- `cassandramq.claimed.total`
- `cassandramq.completed.total`
- `cassandramq.retried.total`
- `cassandramq.failed.total`
- `cassandramq.claim_failures.total`
- `cassandramq.poll_cycles.total`
- `cassandramq.polled_messages.total`
- `cassandramq.owned_shards`
- `cassandramq.inflight_tasks`

## Capacity workflow

1. Fix shard count and bucket size.
2. Scale worker replicas until lease ownership stabilizes.
3. Increase thread pool and inflight limits while watching handler latency.
4. Increase poll/claim budgets until Cassandra latency reaches acceptable envelope.
5. Validate retry and failure rates under load before production rollout.
