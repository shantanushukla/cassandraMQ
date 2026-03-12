package org.cassandramq.util;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardRouterTest {
    @Test
    void shardIsStableAndBounded() {
        UUID id = UUID.fromString("8de4a8b7-3a89-4fcb-85f4-d04b385df832");
        int shard1 = ShardRouter.shardFor("default", id, 128);
        int shard2 = ShardRouter.shardFor("default", id, 128);

        assertEquals(shard1, shard2);
        assertTrue(shard1 >= 0 && shard1 < 128);
    }
}
