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

package uk.org.hecaton.wan.impl;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.management.events.WanConsistencyCheckIgnoredEvent;
import uk.org.hecaton.internal.monitor.WanSyncState;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.wan.WanPublisher;
import uk.org.hecaton.wan.impl.batch.WanBatchPublisher;
import uk.org.hecaton.wan.impl.batch.WanSyncOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static uk.org.hecaton.wan.impl.WanReplicationService.SERVICE_NAME;

/**
 * Starts WAN syncs and consistency checks on every member of the cluster (F13); each member's batch
 * publisher handles the partitions the member owns.
 */
class WanSyncCoordinator {

    private final Node node;
    private final WanReplicationService service;

    WanSyncCoordinator(Node node, WanReplicationService service) {
        this.node = node;
        this.service = service;
    }

    UUID syncMap(String wanReplicationName, String wanPublisherId, String mapName) {
        return start(wanReplicationName, wanPublisherId, List.of(mapName), false);
    }

    UUID syncAllMaps(String wanReplicationName, String wanPublisherId) {
        return start(wanReplicationName, wanPublisherId, mapsReplicatedBy(wanReplicationName), false);
    }

    UUID consistencyCheck(String wanReplicationName, String wanPublisherId, String mapName) {
        if (!Boolean.TRUE.equals(node.getConfig().findMapConfig(mapName).getMerkleTreeConfig().getEnabled())) {
            node.getManagementCenterService().log(new WanConsistencyCheckIgnoredEvent(wanReplicationName, wanPublisherId,
                    mapName, "Map " + mapName + " keeps no Merkle tree"));
            return null;
        }
        return start(wanReplicationName, wanPublisherId, List.of(mapName), true);
    }

    WanSyncState syncState(Map<String, DelegatingWanScheme> schemes) {
        for (Map.Entry<String, DelegatingWanScheme> scheme : schemes.entrySet()) {
            for (WanPublisher publisher : scheme.getValue().getPublishers()) {
                if (publisher instanceof WanBatchPublisher batch && (batch.isSyncing() || batch.hasSyncFailed())) {
                    WanSyncStatus status = batch.isSyncing() ? WanSyncStatus.IN_PROGRESS : WanSyncStatus.FAILED;
                    return new WanSyncStateImpl(status, scheme.getKey(), batch.getPublisherId());
                }
            }
        }
        return new WanSyncStateImpl(WanSyncStatus.READY, null, null);
    }

    private UUID start(String wanReplicationName, String wanPublisherId, List<String> mapNames, boolean check) {
        if (!(service.getPublisherOrFail(wanReplicationName, wanPublisherId) instanceof WanBatchPublisher)) {
            throw new UnsupportedOperationException("WAN publisher " + wanPublisherId + " of " + wanReplicationName
                    + " is a custom publisher, which does not support WAN sync");
        }
        UUID syncId = UuidUtil.newUnsecureUUID();
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        List<InternalCompletableFuture<Object>> futures = new ArrayList<>();
        for (Member member : nodeEngine.getClusterService().getMembers()) {
            futures.add(nodeEngine.getOperationService().invokeOnTarget(SERVICE_NAME,
                    new WanSyncOperation(wanReplicationName, wanPublisherId, syncId, mapNames, check),
                    member.getAddress()));
        }
        futures.forEach(InternalCompletableFuture::joinInternal);
        return syncId;
    }

    /** The maps (with a container on this member) that refer to the WAN replication scheme. */
    private List<String> mapsReplicatedBy(String wanReplicationName) {
        MapService mapService = node.getNodeEngine().getService(MapService.SERVICE_NAME);
        List<String> maps = new ArrayList<>();
        mapService.getMapServiceContext().getMapContainers().forEach((name, container) -> {
            WanReplicationRef ref = container.getMapConfig().getWanReplicationRef();
            if (ref != null && wanReplicationName.equals(ref.getName())) {
                maps.add(name);
            }
        });
        return maps;
    }
}
