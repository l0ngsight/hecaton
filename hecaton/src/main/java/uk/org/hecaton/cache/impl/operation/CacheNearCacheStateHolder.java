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

package uk.org.hecaton.cache.impl.operation;

import uk.org.hecaton.cache.impl.AbstractCacheService;
import uk.org.hecaton.cache.impl.CacheDataSerializerHook;
import uk.org.hecaton.cache.impl.CacheEventHandler;
import uk.org.hecaton.cache.impl.CachePartitionSegment;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MetaDataGenerator;
import uk.org.hecaton.internal.serialization.impl.SerializationUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.emptyList;

/**
 * Holder for Near Cache metadata.
 */
public class CacheNearCacheStateHolder implements IdentifiedDataSerializable {

    private volatile UUID partitionUuid;
    private volatile List<Object> cacheNameSequencePairs = emptyList();
    private Operation cacheReplicationOperation;

    public CacheNearCacheStateHolder() {
    }

    public void setCacheReplicationOperation(Operation cacheReplicationOperation) {
        this.cacheReplicationOperation = cacheReplicationOperation;
    }

    void prepare(CachePartitionSegment segment, Collection<ServiceNamespace> namespaces) {
        ICacheService cacheService = segment.getCacheService();
        MetaDataGenerator metaData = getPartitionMetaDataGenerator(cacheService);

        int partitionId = segment.getPartitionId();
        partitionUuid = metaData.getOrCreateUuid(partitionId);

        cacheNameSequencePairs = Collections.synchronizedList(new ArrayList<>(namespaces.size()));
        for (ServiceNamespace namespace : namespaces) {
            ObjectNamespace ns = (ObjectNamespace) namespace;
            String cacheName = ns.getObjectName();

            cacheNameSequencePairs.add(cacheName);
            cacheNameSequencePairs.add(metaData.currentSequence(cacheName, partitionId));
        }
    }

    private MetaDataGenerator getPartitionMetaDataGenerator(ICacheService cacheService) {
        CacheEventHandler cacheEventHandler = ((AbstractCacheService) cacheService).getCacheEventHandler();
        return cacheEventHandler.getMetaDataGenerator();
    }

    public void applyState() {
        CacheService cacheService = cacheReplicationOperation.getService();
        MetaDataGenerator metaDataGenerator = getPartitionMetaDataGenerator(cacheService);

        int partitionId = cacheReplicationOperation.getPartitionId();

        if (partitionUuid != null) {
            metaDataGenerator.setUuid(partitionId, partitionUuid);
        }

        for (int i = 0; i < cacheNameSequencePairs.size(); ) {
            String cacheName = (String) cacheNameSequencePairs.get(i++);
            long sequence = (Long) cacheNameSequencePairs.get(i++);

            metaDataGenerator.setCurrentSequence(cacheName, partitionId, sequence);
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        boolean nullUuid = partitionUuid == null;
        out.writeBoolean(nullUuid);
        if (!nullUuid) {
            out.writeLong(partitionUuid.getMostSignificantBits());
            out.writeLong(partitionUuid.getLeastSignificantBits());
        }

        SerializationUtil.writeList(cacheNameSequencePairs, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        boolean nullUuid = in.readBoolean();
        partitionUuid = nullUuid ? null : new UUID(in.readLong(), in.readLong());

        cacheNameSequencePairs = Collections.synchronizedList(SerializationUtil.readList(in));
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.CACHE_NEAR_CACHE_STATE_HOLDER;
    }
}
