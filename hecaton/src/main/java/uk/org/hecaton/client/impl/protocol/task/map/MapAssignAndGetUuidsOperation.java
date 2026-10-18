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

package uk.org.hecaton.client.impl.protocol.task.map;

import uk.org.hecaton.internal.nearcache.impl.invalidation.Invalidator;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MetaDataGenerator;
import uk.org.hecaton.map.impl.MapDataSerializerHook;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.nearcache.MapNearCacheManager;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;

import java.util.UUID;

import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;

public class MapAssignAndGetUuidsOperation extends Operation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    private UUID partitionUuid;

    @Override
    public void run() throws Exception {
        partitionUuid = getMetaDataGenerator().getOrCreateUuid(getPartitionId());
    }

    @Override
    public Object getResponse() {
        return partitionUuid;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    private MetaDataGenerator getMetaDataGenerator() {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        return invalidator.getMetaDataGenerator();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MAP_ASSIGN_AND_GET_UUIDS;
    }
}
