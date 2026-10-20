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

package uk.org.hecaton.map.impl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.internal.partition.PartitionAwareService;
import uk.org.hecaton.spi.impl.eventservice.impl.EventServiceImpl;
import uk.org.hecaton.spi.impl.eventservice.impl.EventServiceSegment;
import uk.org.hecaton.internal.partition.IPartitionLostEvent;

import java.util.Set;

/**
 * Defines partition-aware operations' behavior of map service.
 * Currently, it only defines the behavior for partition lost occurrences
 *
 * @see IPartitionLostEvent
 */
class MapPartitionAwareService implements PartitionAwareService {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;
    private final EventServiceImpl eventService;

    MapPartitionAwareService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.eventService = (EventServiceImpl) this.nodeEngine.getEventService();
    }

    @Override
    public void onPartitionLost(IPartitionLostEvent partitionLostEvent) {
        final Address thisAddress = nodeEngine.getThisAddress();
        final int partitionId = partitionLostEvent.getPartitionId();

        EventServiceSegment eventServiceSegment = eventService.getSegment(MapService.SERVICE_NAME, false);
        if (eventServiceSegment == null) {
            return;
        }
        Set<String> maps = eventServiceSegment.getRegistrations().keySet();

        for (String mapName : maps) {
            int totalBackupCount = nodeEngine.getConfig().getMapConfig(mapName).getTotalBackupCount();
            if (totalBackupCount <= partitionLostEvent.getLostReplicaIndex()) {
                mapServiceContext.getMapEventPublisher().publishMapPartitionLostEvent(thisAddress, mapName, partitionId);
            }
        }
    }

}
