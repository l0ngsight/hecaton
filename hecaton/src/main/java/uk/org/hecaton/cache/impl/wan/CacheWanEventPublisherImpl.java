/*
 * Copyright (c) 2026, The Hecaton Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.org.hecaton.cache.impl.wan;

import uk.org.hecaton.cache.CacheEntryView;
import uk.org.hecaton.cache.impl.event.CacheWanEventPublisher;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.wan.impl.DelegatingWanScheme;
import uk.org.hecaton.wan.impl.InternalWanEvent;

import java.util.function.Function;

/**
 * Publishes the WAN events of caches to the WAN replication scheme their configuration refers
 * to: as events of the partition's owner, or, on a backup, as backup events.
 */
public class CacheWanEventPublisherImpl implements CacheWanEventPublisher {

    private final NodeEngine nodeEngine;
    private final Function<String, CacheConfig> cacheConfigs;

    public CacheWanEventPublisherImpl(NodeEngine nodeEngine, Function<String, CacheConfig> cacheConfigs) {
        this.nodeEngine = nodeEngine;
        this.cacheConfigs = cacheConfigs;
    }

    @Override
    public void publishWanUpdate(String cacheNameWithPrefix, CacheEntryView<Data, Data> entryView) {
        WanReplicationRef ref = wanReplicationRef(cacheNameWithPrefix);
        if (ref == null) {
            return;
        }
        WanCacheEntryView<Object, Object> view = new WanCacheEntryView<>(entryView.getKey(), entryView.getValue(),
                entryView.getCreationTime(), entryView.getExpirationTime(), entryView.getLastAccessTime(),
                entryView.getHits(), nodeEngine.getSerializationService());
        publish(ref, new WanCacheUpdateEvent(cacheNameWithPrefix, ref.getMergePolicyClassName(), view));
    }

    @Override
    public void publishWanRemove(String cacheNameWithPrefix, Data key) {
        WanReplicationRef ref = wanReplicationRef(cacheNameWithPrefix);
        if (ref != null) {
            publish(ref, new WanCacheRemoveEvent(cacheNameWithPrefix, key));
        }
    }

    private WanReplicationRef wanReplicationRef(String cacheNameWithPrefix) {
        CacheConfig config = cacheConfigs.apply(cacheNameWithPrefix);
        return config == null ? null : config.getWanReplicationRef();
    }

    private void publish(WanReplicationRef ref, InternalWanEvent<?> event) {
        DelegatingWanScheme scheme = nodeEngine.getWanReplicationService().getWanReplicationPublishers(ref.getName());
        if (scheme == null) {
            return;
        }
        int partitionId = nodeEngine.getPartitionService().getPartitionId(event.getKey());
        if (nodeEngine.getPartitionService().isPartitionOwner(partitionId)) {
            scheme.publishReplicationEvent(event);
        } else {
            scheme.publishReplicationEventBackup(event);
        }
    }
}
