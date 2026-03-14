package org.cassandramq.store;

import com.datastax.oss.driver.api.core.CqlSession;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class SchemaValidator {
    private SchemaValidator() {
    }

    public static void validateRequiredTables(CqlSession session) {
        boolean hasMessages = session.getMetadata()
                .getKeyspace(session.getKeyspace().orElseThrow())
                .flatMap(ks -> ks.getTable("queue_messages_by_shard"))
                .isPresent();
        boolean hasOwnership = session.getMetadata()
                .getKeyspace(session.getKeyspace().orElseThrow())
                .flatMap(ks -> ks.getTable("shard_ownership"))
                .isPresent();

        if (!hasMessages || !hasOwnership) {
            throw new IllegalStateException("Required Cassandra tables are missing. Apply src/main/resources/db/schema.cql first.");
        }
    }
}
