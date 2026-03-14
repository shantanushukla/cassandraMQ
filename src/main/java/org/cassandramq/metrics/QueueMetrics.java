package org.cassandramq.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class QueueMetrics {
    private final AtomicLong claimed = new AtomicLong();
    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong retried = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong recovered = new AtomicLong();
    private final AtomicLong claimFailures = new AtomicLong();
    private final AtomicLong pollCycles = new AtomicLong();
    private final AtomicLong polledMessages = new AtomicLong();
    private final AtomicInteger ownedShards = new AtomicInteger();
    private final AtomicInteger inflightTasks = new AtomicInteger();
    private final AtomicLong queueLagMillis = new AtomicLong();

    private final Counter claimedCounter;
    private final Counter completedCounter;
    private final Counter retriedCounter;
    private final Counter failedCounter;
    private final Counter recoveredCounter;
    private final Counter claimFailuresCounter;
    private final Counter pollCyclesCounter;
    private final Counter polledMessagesCounter;
    private final Timer pollLatencyTimer;
    private final Timer claimLatencyTimer;
    private final Timer handlerLatencyTimer;

    public QueueMetrics() {
        this.claimedCounter = null;
        this.completedCounter = null;
        this.retriedCounter = null;
        this.failedCounter = null;
        this.recoveredCounter = null;
        this.claimFailuresCounter = null;
        this.pollCyclesCounter = null;
        this.polledMessagesCounter = null;
        this.pollLatencyTimer = null;
        this.claimLatencyTimer = null;
        this.handlerLatencyTimer = null;
    }

    public QueueMetrics(MeterRegistry registry, String namespace) {
        this.claimedCounter = Counter.builder(namespace + ".claimed.total").register(registry);
        this.completedCounter = Counter.builder(namespace + ".completed.total").register(registry);
        this.retriedCounter = Counter.builder(namespace + ".retried.total").register(registry);
        this.failedCounter = Counter.builder(namespace + ".failed.total").register(registry);
        this.recoveredCounter = Counter.builder(namespace + ".recovered.total").register(registry);
        this.claimFailuresCounter = Counter.builder(namespace + ".claim_failures.total").register(registry);
        this.pollCyclesCounter = Counter.builder(namespace + ".poll_cycles.total").register(registry);
        this.polledMessagesCounter = Counter.builder(namespace + ".polled_messages.total").register(registry);
        this.pollLatencyTimer = Timer.builder(namespace + ".poll.latency").register(registry);
        this.claimLatencyTimer = Timer.builder(namespace + ".claim.latency").register(registry);
        this.handlerLatencyTimer = Timer.builder(namespace + ".handler.latency").register(registry);
        Gauge.builder(namespace + ".owned_shards", ownedShards, AtomicInteger::get).register(registry);
        Gauge.builder(namespace + ".inflight_tasks", inflightTasks, AtomicInteger::get).register(registry);
        Gauge.builder(namespace + ".queue_lag_ms", queueLagMillis, AtomicLong::get).register(registry);
    }

    public void incrementClaimed() {
        claimed.incrementAndGet();
        increment(claimedCounter);
    }

    public void incrementCompleted() {
        completed.incrementAndGet();
        increment(completedCounter);
    }

    public void incrementRetried() {
        retried.incrementAndGet();
        increment(retriedCounter);
    }

    public void incrementFailed() {
        failed.incrementAndGet();
        increment(failedCounter);
    }

    public void incrementRecovered() {
        recovered.incrementAndGet();
        increment(recoveredCounter);
    }

    public void incrementClaimFailures() {
        claimFailures.incrementAndGet();
        increment(claimFailuresCounter);
    }

    public void incrementPollCycles() {
        pollCycles.incrementAndGet();
        increment(pollCyclesCounter);
    }

    public void incrementPolledMessages() {
        polledMessages.incrementAndGet();
        increment(polledMessagesCounter);
    }

    public void setOwnedShards(int count) {
        ownedShards.set(Math.max(count, 0));
    }

    public void setInflightTasks(int count) {
        inflightTasks.set(Math.max(count, 0));
    }

    public void setQueueLagMillis(long millis) {
        queueLagMillis.set(Math.max(millis, 0));
    }

    public void recordPollLatency(Duration duration) {
        record(pollLatencyTimer, duration);
    }

    public void recordClaimLatency(Duration duration) {
        record(claimLatencyTimer, duration);
    }

    public void recordHandlerLatency(Duration duration) {
        record(handlerLatencyTimer, duration);
    }

    public long claimed() {
        return claimed.get();
    }

    public long completed() {
        return completed.get();
    }

    public long retried() {
        return retried.get();
    }

    public long failed() {
        return failed.get();
    }

    public long recovered() {
        return recovered.get();
    }

    public long claimFailures() {
        return claimFailures.get();
    }

    public long pollCycles() {
        return pollCycles.get();
    }

    public long polledMessages() {
        return polledMessages.get();
    }

    public int inflightTasks() {
        return inflightTasks.get();
    }

    public int ownedShards() {
        return ownedShards.get();
    }

    public long queueLagMillis() {
        return queueLagMillis.get();
    }

    private void increment(Counter counter) {
        if (counter != null) {
            counter.increment();
        }
    }

    private void record(Timer timer, Duration duration) {
        if (timer != null) {
            timer.record(duration);
        }
    }
}
