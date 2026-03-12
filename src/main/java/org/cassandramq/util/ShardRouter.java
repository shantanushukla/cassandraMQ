package org.cassandramq.util;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class ShardRouter {
    private ShardRouter() {
    }

    public static int shardFor(String queueName, UUID messageId, int totalShards) {
        String key = queueName + ':' + messageId;
        int hash = java.util.Arrays.hashCode(key.getBytes(StandardCharsets.UTF_8));
        return Math.floorMod(hash, totalShards);
    }
}
