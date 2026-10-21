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

package uk.org.hecaton.map.impl.nearcache;

import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.instance.module.NativeMemoryCapability;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.internal.nearcache.NearCacheRecordStore;
import uk.org.hecaton.internal.nearcache.impl.DefaultNearCache;
import uk.org.hecaton.internal.nearcache.impl.DefaultNearCacheManager;
import uk.org.hecaton.internal.nearcache.impl.invalidation.BatchInvalidator;
import uk.org.hecaton.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher;
import uk.org.hecaton.internal.nearcache.impl.invalidation.Invalidator;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MinimalPartitionService;
import uk.org.hecaton.internal.nearcache.impl.invalidation.NonStopInvalidator;
import uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingHandler;
import uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.EventListenerFilter;
import uk.org.hecaton.map.impl.MapManagedService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.nearcache.invalidation.MemberMapInvalidationMetaDataFetcher;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.eventservice.EventFilter;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.executionservice.ExecutionService;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.properties.HecatonProperties;

import java.util.UUID;
import java.util.function.Predicate;

import static uk.org.hecaton.core.EntryEventType.INVALIDATION;
import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;

public class MapNearCacheManager extends DefaultNearCacheManager {

    /**
     * Filters out listeners other than invalidation related ones.
     */
    private static final InvalidationAcceptorFilter INVALIDATION_ACCEPTOR = new InvalidationAcceptorFilter();

    protected final int partitionCount;
    protected final NodeEngine nodeEngine;
    protected final MapServiceContext mapServiceContext;
    protected final MinimalPartitionService partitionService;
    protected final Invalidator invalidator;
    protected final RepairingTask repairingTask;

    public MapNearCacheManager(MapServiceContext mapServiceContext) {
        super(mapServiceContext.getNodeEngine().getSerializationService(),
                mapServiceContext.getNodeEngine().getExecutionService().getGlobalTaskScheduler(),
                null, mapServiceContext.getNodeEngine().getProperties());
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.mapServiceContext = mapServiceContext;
        this.partitionService = new MemberMinimalPartitionService(nodeEngine.getPartitionService());
        this.partitionCount = partitionService.getPartitionCount();
        this.invalidator = createInvalidator();
        this.repairingTask = createRepairingInvalidationTask();
    }

    @Override
    protected <K, V> NearCache<K, V> createNearCache(String name, NearCacheConfig nearCacheConfig) {
        if (nearCacheConfig.getInMemoryFormat() != InMemoryFormat.NATIVE) {
            return super.createNearCache(name, nearCacheConfig);
        }
        NearCacheRecordStore<K, V> recordStore = ((NodeEngineImpl) nodeEngine).getNode().getMemberModules()
                .require(NativeMemoryCapability.class, "NATIVE in-memory format")
                .createNearCacheRecordStore(name, nearCacheConfig, serializationService, classLoader, properties);
        return new DefaultNearCache<>(name, nearCacheConfig, recordStore, serializationService, scheduler, classLoader,
                properties);
    }

    private Invalidator createInvalidator() {
        HecatonProperties hecatonProperties = nodeEngine.getProperties();
        int batchSize = hecatonProperties.getInteger(MAP_INVALIDATION_MESSAGE_BATCH_SIZE);
        int batchFrequencySeconds = hecatonProperties.getInteger(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS);
        boolean batchingEnabled = hecatonProperties.getBoolean(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED) && batchSize > 1;

        if (batchingEnabled) {
            return new BatchInvalidator(SERVICE_NAME, batchSize, batchFrequencySeconds, INVALIDATION_ACCEPTOR, nodeEngine);
        } else {
            return new NonStopInvalidator(SERVICE_NAME, INVALIDATION_ACCEPTOR, nodeEngine);
        }
    }

    /**
     * Filters out listeners other than invalidation related ones.
     */
    private static class InvalidationAcceptorFilter implements Predicate<EventRegistration> {

        @Override
        public boolean test(EventRegistration eventRegistration) {
            EventFilter filter = eventRegistration.getFilter();
            return filter instanceof EventListenerFilter && filter.eval(INVALIDATION.getType());
        }
    }

    private RepairingTask createRepairingInvalidationTask() {
        ExecutionService executionService = nodeEngine.getExecutionService();
        ClusterService clusterService = nodeEngine.getClusterService();
        OperationService operationService = nodeEngine.getOperationService();
        HecatonProperties properties = nodeEngine.getProperties();

        ILogger metadataFetcherLogger = nodeEngine.getLogger(MemberMapInvalidationMetaDataFetcher.class);
        InvalidationMetaDataFetcher invalidationMetaDataFetcher
                = new MemberMapInvalidationMetaDataFetcher(clusterService, operationService, metadataFetcherLogger);

        ILogger repairingTaskLogger = nodeEngine.getLogger(RepairingTask.class);
        UUID localUuid = nodeEngine.getLocalMember().getUuid();
        return new RepairingTask(properties, invalidationMetaDataFetcher, executionService.getGlobalTaskScheduler(),
                serializationService, partitionService, localUuid, repairingTaskLogger);
    }

    /**
     * @see MapManagedService#reset()
     */
    public void reset() {
        clearAllNearCaches();
        invalidator.reset();
    }

    /**
     * @see MapManagedService#shutdown(boolean)
     */
    public void shutdown() {
        destroyAllNearCaches();
        invalidator.shutdown();
    }

    /**
     * @see RemoteService#destroyDistributedObject(String)
     */
    @Override
    public boolean destroyNearCache(String mapName) {
        invalidator.destroy(mapName, nodeEngine.getLocalMember().getUuid());
        return super.destroyNearCache(mapName);
    }

    public Invalidator getInvalidator() {
        return invalidator;
    }

    public RepairingHandler newRepairingHandler(String name, NearCache nearCache) {
        return repairingTask.registerAndGetHandler(name, nearCache);
    }

    public void deregisterRepairingHandler(String name) {
        repairingTask.deregisterHandler(name);
    }

    // used in tests
    public RepairingTask getRepairingTask() {
        return repairingTask;
    }
}
