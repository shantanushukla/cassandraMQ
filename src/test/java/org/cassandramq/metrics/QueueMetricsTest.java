package org.cassandramq.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QueueMetricsTest {
    @Test
    void incrementsCountersAndTracksGauges() {
        QueueMetrics metrics = new QueueMetrics();

        metrics.incrementClaimed();
        metrics.incrementCompleted();
        metrics.incrementRetried();
        metrics.incrementFailed();
        metrics.incrementRecovered();
        metrics.incrementClaimFailures();
        metrics.incrementPollCycles();
        metrics.incrementPolledMessages();
        metrics.setInflightTasks(5);
        metrics.setOwnedShards(3);
        metrics.setQueueLagMillis(1200);

        assertEquals(1, metrics.claimed());
        assertEquals(1, metrics.completed());
        assertEquals(1, metrics.retried());
        assertEquals(1, metrics.failed());
        assertEquals(1, metrics.recovered());
        assertEquals(1, metrics.claimFailures());
        assertEquals(1, metrics.pollCycles());
        assertEquals(1, metrics.polledMessages());
        assertEquals(5, metrics.inflightTasks());
        assertEquals(3, metrics.ownedShards());
        assertEquals(1200, metrics.queueLagMillis());
    }

    @Test
    void clampsNegativeGaugeValuesToZero() {
        QueueMetrics metrics = new QueueMetrics();

        metrics.setInflightTasks(-2);
        metrics.setOwnedShards(-1);
        metrics.setQueueLagMillis(-5);

        assertEquals(0, metrics.inflightTasks());
        assertEquals(0, metrics.ownedShards());
        assertEquals(0, metrics.queueLagMillis());
    }
}
