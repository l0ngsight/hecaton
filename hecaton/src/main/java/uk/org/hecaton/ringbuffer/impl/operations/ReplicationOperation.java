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

package uk.org.hecaton.ringbuffer.impl.operations;

import uk.org.hecaton.cache.CacheNotExistsException;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.journal.CacheEventJournal;
import uk.org.hecaton.config.EventJournalConfig;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.internal.namespace.impl.NodeEngineThreadLocalContext;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.journal.MapEventJournal;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.ringbuffer.impl.RingbufferContainer;
import uk.org.hecaton.ringbuffer.impl.RingbufferService;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static uk.org.hecaton.ringbuffer.impl.RingbufferDataSerializerHook.F_ID;
import static uk.org.hecaton.ringbuffer.impl.RingbufferDataSerializerHook.REPLICATION_OPERATION;
import static uk.org.hecaton.ringbuffer.impl.RingbufferService.SERVICE_NAME;
import static uk.org.hecaton.internal.util.MapUtil.createHashMap;

public class ReplicationOperation extends Operation implements IdentifiedDataSerializable {

    private Map<ObjectNamespace, RingbufferContainer> migrationData;

    public ReplicationOperation() {
    }

    public ReplicationOperation(Map<ObjectNamespace, RingbufferContainer> migrationData,
                                int partitionId, int replicaIndex) {
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        this.migrationData = migrationData;
    }

    @Override
    public void run() {
        final RingbufferService service = getService();
        for (Map.Entry<ObjectNamespace, RingbufferContainer> entry : migrationData.entrySet()) {
            final ObjectNamespace ns = entry.getKey();
            final RingbufferContainer ringbuffer = entry.getValue();
            service.addRingbuffer(getPartitionId(), ringbuffer, getRingbufferConfig(service, ns));
        }
    }

    /**
     * Returns the ringbuffer config for the provided namespace. The namespace
     * provides information whether the requested ringbuffer is a ringbuffer
     * that the user is directly interacting with through a ringbuffer proxy
     * or if this is a backing ringbuffer for an event journal.
     * If a ringbuffer configuration for an event journal is requested, this
     * method will expect the configuration for the relevant map or cache
     * to be available.
     *
     * @param service the ringbuffer service
     * @param ns      the object namespace for which we are creating a ringbuffer
     * @return the ringbuffer configuration
     * @throws CacheNotExistsException if a config for a cache event journal was requested
     *                                 and the cache configuration was not found
     */
    private RingbufferConfig getRingbufferConfig(RingbufferService service, ObjectNamespace ns) {
        final String serviceName = ns.getServiceName();
        if (RingbufferService.SERVICE_NAME.equals(serviceName)) {
            return service.getRingbufferConfig(ns.getObjectName());
        } else if (MapService.SERVICE_NAME.equals(serviceName)) {
            final MapService mapService = getNodeEngine().getService(MapService.SERVICE_NAME);
            final MapEventJournal journal = mapService.getMapServiceContext().getEventJournal();
            final EventJournalConfig journalConfig = journal.getEventJournalConfig(ns);
            return journal.toRingbufferConfig(journalConfig, ns);
        } else if (CacheService.SERVICE_NAME.equals(serviceName)) {
            final CacheService cacheService = getNodeEngine().getService(CacheService.SERVICE_NAME);
            final CacheEventJournal journal = cacheService.getEventJournal();
            final EventJournalConfig journalConfig = journal.getEventJournalConfig(ns);
            return journal.toRingbufferConfig(journalConfig, ns);
        } else {
            throw new IllegalArgumentException("Unsupported ringbuffer service name " + serviceName);
        }
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getClassId() {
        return REPLICATION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Entry<ObjectNamespace, RingbufferContainer> entry : migrationData.entrySet()) {
            final ObjectNamespace ns = entry.getKey();
            out.writeObject(ns);
            RingbufferContainer container = entry.getValue();
            container.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        migrationData = createHashMap(mapSize);
        NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
        RingbufferService service = engine.getService(SERVICE_NAME);
        for (int i = 0; i < mapSize; i++) {
            final ObjectNamespace namespace = in.readObject();
            assert namespace != null;
            RingbufferConfig ringbufferConfig = service.getRingbufferConfig(namespace.getObjectName());
            String userCodeNamespace = ringbufferConfig.getUserCodeNamespace();
            final var container = new RingbufferContainer<>(namespace, getPartitionId(), userCodeNamespace);
            container.readData(in);
            migrationData.put(namespace, container);
        }
    }
}
