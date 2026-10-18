/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.map.impl.wan;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.WanConsumerConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.config.WanSyncConfig;
import uk.org.hecaton.jet.impl.util.ConcurrentMemoizingSupplier;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicyProvider;
import uk.org.hecaton.wan.impl.DelegatingWanScheme;
import uk.org.hecaton.wan.impl.WanReplicationService;

import static uk.org.hecaton.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static uk.org.hecaton.internal.config.MergePolicyValidator.checkMapMergePolicy;
import static java.lang.Boolean.TRUE;

public class MapWanContext {

    protected volatile SplitBrainMergePolicy wanMergePolicy;
    protected volatile ConcurrentMemoizingSupplier<DelegatingWanScheme> wanReplicationDelegateSupplier;
    private final String name;
    private final MapServiceContext mapServiceContext;
    private volatile MapConfig mapConfig;
    private volatile boolean persistWanReplicatedData;

    public MapWanContext(MapContainer mapContainer) {
        this.mapConfig = mapContainer.getMapConfig();
        this.name = mapContainer.getName();
        this.mapServiceContext = mapContainer.getMapServiceContext();
    }

    /**
     * Used to update the {@link MapConfig} reference to match that of
     * this context's owning {@link MapContainer}.
     *
     * @param mapConfig the new {@link MapConfig} to apply
     */
    public void setMapConfig(MapConfig mapConfig) {
        this.mapConfig = mapConfig;
    }

    public void start() {
        initWanReplication();
    }

    private void initWanReplication() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
        if (wanReplicationRef == null) {
            // reset due to possible reconfiguration
            wanReplicationDelegateSupplier = null;
            return;
        }
        String wanReplicationRefName = wanReplicationRef.getName();

        Config config = nodeEngine.getConfig();
        if (!TRUE.equals(mapConfig.getMerkleTreeConfig().getEnabled())
                && hasPublisherWithMerkleTreeSync(config, wanReplicationRefName)) {
            throw new InvalidConfigurationException(
                    "Map " + name + " has disabled merkle trees but the WAN replication scheme "
                            + wanReplicationRefName + " has publishers that use merkle trees."
                            + " Please enable merkle trees for the map.");
        }

        WanReplicationService wanReplicationService = nodeEngine.getWanReplicationService();
        // reset due to possible reconfiguration
        wanReplicationDelegateSupplier = null;
        if (wanReplicationService.hasWanReplicationScheme(wanReplicationRefName)) {
            wanReplicationDelegateSupplier = new ConcurrentMemoizingSupplier<>(() ->
                    wanReplicationService.getWanReplicationPublishers(wanReplicationRefName)
            );
        }
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        wanMergePolicy = mergePolicyProvider.getMergePolicy(
                wanReplicationRef.getMergePolicyClassName(),
                mapConfig.getUserCodeNamespace()
        );
        checkMapMergePolicy(mapConfig, wanReplicationRef.getMergePolicyClassName(), mergePolicyProvider);

        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(wanReplicationRefName);
        if (wanReplicationConfig != null) {
            WanConsumerConfig wanConsumerConfig = wanReplicationConfig.getConsumerConfig();
            if (wanConsumerConfig != null) {
                persistWanReplicatedData = wanConsumerConfig.isPersistWanReplicatedData();
            }
        }
    }

    /**
     * Returns {@code true} if at least one of the WAN publishers has
     * Merkle tree consistency check configured for the given WAN
     * replication configuration
     *
     * @param config                configuration
     * @param wanReplicationRefName The name of the WAN replication
     * @return {@code true} if there is at least one publisher has Merkle
     * tree configured
     */
    private boolean hasPublisherWithMerkleTreeSync(Config config, String wanReplicationRefName) {
        WanReplicationConfig replicationConfig = config.getWanReplicationConfig(wanReplicationRefName);
        if (replicationConfig == null) {
            return false;
        }
        return replicationConfig.getBatchPublisherConfigs()
                .stream()
                .anyMatch(c -> {
                    WanSyncConfig syncConfig = c.getSyncConfig();
                    return syncConfig != null && MERKLE_TREES == syncConfig.getConsistencyCheckStrategy();
                });
    }

    public DelegatingWanScheme getWanReplicationDelegate() {
        if (wanReplicationDelegateSupplier == null) {
            return null;
        }

        return wanReplicationDelegateSupplier.get();
    }

    public SplitBrainMergePolicy getWanMergePolicy() {
        return wanMergePolicy;
    }

    public boolean isWanReplicationEnabled() {
        return wanReplicationDelegateSupplier != null && wanMergePolicy != null;
    }

    public boolean isWanRepublishingEnabled() {
        return isWanReplicationEnabled() && mapConfig.getWanReplicationRef().isRepublishingEnabled();
    }

    public boolean isPersistWanReplicatedData() {
        return persistWanReplicatedData;
    }
}
