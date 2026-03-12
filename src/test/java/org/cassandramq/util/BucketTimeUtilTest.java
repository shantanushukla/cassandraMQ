package org.cassandramq.util;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BucketTimeUtilTest {
    @Test
    void bucketStartRoundsDownToBucketBoundary() {
        Instant ts = Instant.parse("2026-03-08T10:15:39Z");
        Instant bucket = BucketTimeUtil.bucketStart(ts, 10);
        assertEquals(Instant.parse("2026-03-08T10:15:30Z"), bucket);
    }
}
