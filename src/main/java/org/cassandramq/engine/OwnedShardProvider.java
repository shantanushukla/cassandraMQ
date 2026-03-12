package org.cassandramq.engine;

import java.util.Set;

public interface OwnedShardProvider {
    Set<Integer> ownedShardsSnapshot();
}
