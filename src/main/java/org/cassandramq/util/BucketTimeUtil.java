package org.cassandramq.util;

import java.time.Instant;

public final class BucketTimeUtil {
    private BucketTimeUtil() {
    }

    public static Instant bucketStart(Instant timestamp, int bucketSizeSeconds) {
        long epoch = timestamp.getEpochSecond();
        long bucketEpoch = (epoch / bucketSizeSeconds) * bucketSizeSeconds;
        return Instant.ofEpochSecond(bucketEpoch);
    }
}
