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

import uk.org.hecaton.config.CacheDeserializedValues;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EventJournalConfig;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.nio.ClassLoaderUtil;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.internal.services.PostJoinAwareService;
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.internal.util.ExceptionUtil;
import uk.org.hecaton.internal.util.MemoryInfoAccessor;
import uk.org.hecaton.internal.util.RuntimeMemoryInfoAccessor;
import uk.org.hecaton.internal.util.ThreadUtil;
import uk.org.hecaton.map.impl.eviction.EvictionChecker;
import uk.org.hecaton.map.impl.eviction.Evictor;
import uk.org.hecaton.map.impl.eviction.EvictorImpl;
import uk.org.hecaton.map.impl.mapstore.MapStoreContext;
import uk.org.hecaton.map.impl.nearcache.invalidation.InvalidationListener;
import uk.org.hecaton.map.impl.query.QueryEntryFactory;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.map.impl.wan.MapWanContext;
import uk.org.hecaton.partition.PartitioningStrategy;
import uk.org.hecaton.query.impl.IndexRegistry;
import uk.org.hecaton.query.impl.InternalIndex;
import uk.org.hecaton.query.impl.QueryableEntry;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.spi.eviction.EvictionPolicyComparator;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.properties.HecatonProperties;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static uk.org.hecaton.config.InMemoryFormat.NATIVE;
import static uk.org.hecaton.config.InMemoryFormat.OBJECT;
import static uk.org.hecaton.internal.eviction.EvictionPolicyEvaluatorProvider.getEvictionPolicyComparator;
import static uk.org.hecaton.map.impl.eviction.Evictor.NULL_EVICTOR;
import static uk.org.hecaton.map.impl.mapstore.MapStoreContextFactory.createMapStoreContext;
import static uk.org.hecaton.query.impl.InternalIndex.GLOBAL_INDEX_NOOP_PARTITION_ID;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_EVICTION_BATCH_SIZE;
import static java.lang.System.getProperty;

/**
 * Map container for a map with a specific name. Contains config and
 * supporting structures for all of the maps' functionalities.
 */
@SuppressWarnings({"WeakerAccess", "checkstyle:classfanoutcomplexity", "MethodCount"})
public class MapContainerImpl implements MapContainer {

    protected final String name;
    protected final String splitBrainProtectionName;
    // on-heap indexes are global, meaning there is only one index per map,
    // stored in the mapContainer, so if globalIndexRegistry is null it means that
    // global index is not in use
    protected final Extractors extractors;
    protected final MapStoreContext mapStoreContext;
    protected final ObjectNamespace objectNamespace;
    protected final IndexRegistry globalIndexRegistry;
    protected final MapServiceContext mapServiceContext;
    protected final QueryEntryFactory queryEntryFactory;
    protected final EventJournalConfig eventJournalConfig;
    protected final PartitioningStrategy partitioningStrategy;
    protected final InternalSerializationService serializationService;
    protected final Function<Object, Data> toDataFunction = new ObjectToData();
    protected final InterceptorRegistry interceptorRegistry = new InterceptorRegistry();
    protected final ConcurrentMap<Integer, IndexRegistry> partitionedIndexRegistry = new ConcurrentHashMap<>();

    /**
     * Holds number of registered {@link InvalidationListener} from clients.
     */
    protected final AtomicInteger invalidationListenerCount = new AtomicInteger();
    protected final AtomicLong lastInvalidMergePolicyCheckTime = new AtomicLong();
    protected final AtomicBoolean onDestroyCalled = new AtomicBoolean();

    protected volatile MapConfig mapConfig;
    private volatile Evictor evictor;

    private final MapWanContext wanContext;
    private final boolean queryExpirationCheckEnabled;

    private volatile boolean destroyed;

    /**
     * Operations which are done in this constructor should obey the rules defined
     * in the method comment {@link PostJoinAwareService#getPostJoinOperation()}
     * Otherwise undesired situations, like deadlocks, may appear.
     */
    @SuppressWarnings("checkstyle:executablestatementcount")
    public MapContainerImpl(final String name, final Config config, final MapServiceContext mapServiceContext) {
        this.name = name;
        this.mapConfig = config.findMapConfig(name);
        this.eventJournalConfig = mapConfig.getEventJournalConfig();
        this.mapServiceContext = mapServiceContext;
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.partitioningStrategy = createPartitioningStrategy();
        this.splitBrainProtectionName = mapConfig.getSplitBrainProtectionName();
        this.serializationService = ((InternalSerializationService) nodeEngine.getSerializationService());
        this.objectNamespace = MapService.getObjectNamespace(name);
        this.extractors = Extractors.newBuilder(serializationService)
                .setAttributeConfigs(mapConfig.getAttributeConfigs())
                .setClassLoader(NamespaceUtil.getClassLoaderForNamespace(nodeEngine, mapConfig.getUserCodeNamespace()))
                .build();
        this.queryEntryFactory = new QueryEntryFactory(mapConfig.getCacheDeserializedValues(),
                serializationService, extractors);
        // Read queryExpirationCheckEnabled before creating globalIndexRegistry
        HecatonProperties properties = nodeEngine.getProperties();
        this.queryExpirationCheckEnabled = properties.getBoolean(QUERY_EXPIRATION_CHECK_ENABLED);
        this.globalIndexRegistry = shouldUseGlobalIndex()
                ? createIndexRegistry(true, GLOBAL_INDEX_NOOP_PARTITION_ID) : null;
        this.mapStoreContext = createMapStoreContext(this);
        this.wanContext = new MapWanContext(this);
    }

    @Override
    public void init() {
        initEvictor();
        mapStoreContext.start();
        wanContext.start();
    }

    /**
     * @param global      set {@code true} to create global indexes, otherwise set
     *                    {@code false} to have partitioned indexes
     * @param partitionId the partition ID the index is created on. {@code -1}
     *                    for global indexes.
     * @return a new IndexRegistry object
     */
    @Override
    public IndexRegistry createIndexRegistry(boolean global, int partitionId) {
        int partitionCount = mapServiceContext.getNodeEngine().getPartitionService().getPartitionCount();

        Node node = mapServiceContext.getNodeEngine().getNode();
        return IndexRegistry.newBuilder(node, getName(),
                        serializationService, mapServiceContext.getIndexCopyBehavior(),
                        mapConfig.getInMemoryFormat())
                .global(global)
                .extractors(extractors)
                .statsEnabled(mapConfig.isStatisticsEnabled())
                .indexProvider(mapServiceContext.getIndexProvider(mapConfig))
                .usesCachedQueryableEntries(mapConfig.getCacheDeserializedValues() != CacheDeserializedValues.NEVER)
                .partitionCount(partitionCount)
                .partitionId(partitionId)
                .resultFilterFactory(queryExpirationCheckEnabled
                        ? new IndexResultFilterFactory() : null)
                .build();
    }

    // -------------------------------------------------------------------------------------------------------------
    // IMPORTANT: never use directly! use MapContainer.getIndex() instead.
    // There are cases where a global index is used. In this case, the global-index is stored in the MapContainer.
    // By using this method in the context of global index an exception will be thrown.
    // -------------------------------------------------------------------------------------------------------------
    private IndexRegistry getOrCreatePartitionedIndexRegistry(int partitionId) {
        if (shouldUseGlobalIndex()) {
            throw new IllegalStateException("Can't use a partitioned-index in the context of a global-index.");
        }

        return partitionedIndexRegistry.computeIfAbsent(partitionId,
                integer -> createIndexRegistry(false, partitionId));
    }

    @Override
    public AtomicLong getLastInvalidMergePolicyCheckTime() {
        return lastInvalidMergePolicyCheckTime;
    }

    private class IndexResultFilterFactory implements Supplier<Predicate<QueryableEntry>> {

        @Override
        public Predicate<QueryableEntry> get() {
            return new Predicate<>() {
                private final long nowInMillis = Clock.currentTimeMillis();

                @Override
                public boolean test(QueryableEntry queryableEntry) {
                    return MapContainerImpl.this.hasNotExpired(queryableEntry, nowInMillis);
                }
            };
        }
    }

    /**
     * @return {@code true} if queryableEntry has
     * not expired, otherwise returns {@code false}
     */
    private boolean hasNotExpired(QueryableEntry queryableEntry, long now) {
        Data keyData = queryableEntry.getKeyData();
        IPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
        int partitionId = partitionService.getPartitionId(keyData);

        if (!getOrCreateIndexRegistry(partitionId).isGlobal()) {
            ThreadUtil.assertRunningOnPartitionThread();
        }

        RecordStore recordStore = mapServiceContext.getExistingRecordStore(partitionId, name);
        return recordStore != null
                && !recordStore.isExpired(keyData, now, false);
    }

    @Override
    public final void initEvictor() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        EvictionPolicyComparator evictionPolicyComparator
                = getEvictionPolicyComparator(mapConfig.getEvictionConfig(), nodeEngine.getConfigClassLoader());

        evictor = evictionPolicyComparator != null
                ? newEvictor(evictionPolicyComparator, nodeEngine.getProperties().getInteger(MAP_EVICTION_BATCH_SIZE),
                nodeEngine.getPartitionService()) : NULL_EVICTOR;
    }

    // this method is overridden
    protected Evictor newEvictor(EvictionPolicyComparator evictionPolicyComparator,
                                 int evictionBatchSize, IPartitionService partitionService) {
        EvictionChecker evictionChecker = new EvictionChecker(getMemoryInfoAccessor(), mapServiceContext);

        return new EvictorImpl(evictionPolicyComparator, evictionChecker, evictionBatchSize, partitionService);
    }

    @Override
    public boolean shouldUseGlobalIndex() {
        return mapConfig.getInMemoryFormat() != NATIVE
                || (!mapConfig.getTieredStoreConfig().isEnabled() && mapServiceContext.globalIndexEnabled())
                || mapServiceContext.isForciblyEnabledGlobalIndex();
    }

    protected static MemoryInfoAccessor getMemoryInfoAccessor() {
        MemoryInfoAccessor pluggedMemoryInfoAccessor = getPluggedMemoryInfoAccessor();
        return pluggedMemoryInfoAccessor != null ? pluggedMemoryInfoAccessor : new RuntimeMemoryInfoAccessor();
    }

    private static MemoryInfoAccessor getPluggedMemoryInfoAccessor() {
        String memoryInfoAccessorImpl = getProperty("hecaton.memory.info.accessor.impl");
        if (memoryInfoAccessorImpl == null) {
            return null;
        }

        try {
            return ClassLoaderUtil.newInstance(null, memoryInfoAccessorImpl);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private PartitioningStrategy createPartitioningStrategy() {
        return mapServiceContext.getPartitioningStrategy(
                mapConfig.getName(),
                mapConfig.getPartitioningStrategyConfig(),
                mapConfig.getPartitioningAttributeConfigs()
        );
    }

    /**
     * Used to get index registry of one
     * of global or partitioned indexes.
     *
     * @return by default always returns global-index
     * registry otherwise return partitioned-index registry
     */
    @Override
    public IndexRegistry getOrCreateIndexRegistry(int partitionId) {
        if (globalIndexRegistry != null) {
            return globalIndexRegistry;
        }

        return getOrCreatePartitionedIndexRegistry(partitionId);
    }

    /**
     * @return the global index, if the global index is in use or null.
     */
    @Override
    public IndexRegistry getGlobalIndexRegistry() {
        return globalIndexRegistry;
    }

    @Nullable
    @Override
    public IndexRegistry getOrNullPartitionedIndexRegistry(int partitionId) {
        return partitionedIndexRegistry.get(partitionId);
    }

    // Only used for testing
    @Override
    public ConcurrentMap<Integer, IndexRegistry> getPartitionedIndexRegistry() {
        return partitionedIndexRegistry;
    }

    // Only used for testing
    @Override
    public boolean isEmptyIndexRegistry() {
        if (globalIndexRegistry != null) {
            return globalIndexRegistry.getIndexes().length == 0;
        }
        return partitionedIndexRegistry.isEmpty();
    }

    @Override
    public MapWanContext getWanContext() {
        return wanContext;
    }


    @Override
    public int getTotalBackupCount() {
        return getBackupCount() + getAsyncBackupCount();
    }

    @Override
    public int getBackupCount() {
        return mapConfig.getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return mapConfig.getAsyncBackupCount();
    }

    @Override
    public PartitioningStrategy getPartitioningStrategy() {
        return partitioningStrategy;
    }

    @Override
    public MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }

    @Override
    public MapStoreContext getMapStoreContext() {
        return mapStoreContext;
    }

    @Override
    public MapConfig getMapConfig() {
        return mapConfig;
    }

    @Override
    public void setMapConfig(MapConfig mapConfig) {
        this.mapConfig = mapConfig;
    }

    @Override
    public EventJournalConfig getEventJournalConfig() {
        return eventJournalConfig;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getSplitBrainProtectionName() {
        return splitBrainProtectionName;
    }

    @Override
    public Function<Object, Data> toData() {
        return toDataFunction;
    }

    @Override
    public QueryableEntry newQueryEntry(Data key, Object value) {
        return queryEntryFactory.newEntry(key, value);
    }

    @Override
    public Evictor getEvictor() {
        return evictor;
    }

    // only used for testing purposes
    @Override
    public void setEvictor(Evictor evictor) {
        this.evictor = evictor;
    }

    @Override
    public Extractors getExtractors() {
        return extractors;
    }

    @Override
    public boolean hasInvalidationListener() {
        return invalidationListenerCount.get() > 0;
    }

    @Override
    public AtomicInteger getInvalidationListenerCounter() {
        return invalidationListenerCount;
    }

    @Override
    public void increaseInvalidationListenerCount() {
        invalidationListenerCount.incrementAndGet();
    }

    @Override
    public void decreaseInvalidationListenerCount() {
        invalidationListenerCount.decrementAndGet();
    }

    @Override
    public InterceptorRegistry getInterceptorRegistry() {
        return interceptorRegistry;
    }

    /**
     * Callback invoked before record store and indexes are destroyed. Ensures that if map iterator observes a non-destroyed
     * state, then associated data structures are still valid.
     */
    @Override
    public void onBeforeDestroy() {
        destroyed = true;
    }

    // callback called when the MapContainer is de-registered
    // from MapService and destroyed - basically on map-destroy
    @Override
    public final void onDestroy() {
        if (!onDestroyCalled.compareAndSet(false, true)) {
            return;
        }

        onDestroyInternal();
    }

    protected void onDestroyInternal() {
        if (shouldUseGlobalIndex()) {
            destroyGlobalIndexes();
        }

        mapServiceContext.getLocalMapStatsProvider()
                .destroyLocalMapStatsImpl(getName());
    }

    protected void destroyGlobalIndexes() {
        getGlobalIndexRegistry().destroyIndexes();
    }

    @Override
    public boolean isDestroyed() {
        return destroyed;
    }

    @Override
    public boolean shouldCloneOnEntryProcessing(int partitionId) {
        return getOrCreateIndexRegistry(partitionId).haveAtLeastOneIndex()
                && OBJECT == mapConfig.getInMemoryFormat();
    }

    @Override
    public ObjectNamespace getObjectNamespace() {
        return objectNamespace;
    }

    @Override
    public Map<String, IndexConfig> getIndexDefinitions() {
        return shouldUseGlobalIndex()
                ? getGlobalIndexDefinitions()
                : getPartitionedIndexDefinitions();
    }

    private Map<String, IndexConfig> getGlobalIndexDefinitions() {
        Map<String, IndexConfig> definitions = new HashMap<>();
        InternalIndex[] indexes = globalIndexRegistry.getIndexes();
        for (int i = 0; i < indexes.length; i++) {
            definitions.put(indexes[i].getName(), indexes[i].getConfig());
        }
        return definitions;
    }

    private Map<String, IndexConfig> getPartitionedIndexDefinitions() {
        Map<String, IndexConfig> definitions = new HashMap<>();
        int partitionCount = mapServiceContext.getNodeEngine().getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            IndexRegistry indexRegistry = getOrNullPartitionedIndexRegistry(i);
            if (indexRegistry == null) {
                continue;
            }

            InternalIndex[] indexes = indexRegistry.getIndexes();
            for (int j = 0; j < indexes.length; j++) {
                definitions.put(indexes[j].getName(), indexes[j].getConfig());
            }
        }
        return definitions;
    }

    private class ObjectToData implements Function<Object, Data> {
        @Override
        public Data apply(Object input) {
            SerializationService ss = mapStoreContext.getSerializationService();
            return ss.toData(input, partitioningStrategy);
        }

    }

    @Override
    public boolean isUseCachedDeserializedValuesEnabled(int partitionId) {
        switch (getMapConfig().getCacheDeserializedValues()) {
            case NEVER:
                return false;
            case ALWAYS:
                return true;
            default:
                //if index exists then cached value is already set -> let's use it
                return getOrCreateIndexRegistry(partitionId).haveAtLeastOneIndex();
        }
    }

    // Only for testing purposes
    public boolean isQueryExpirationCheckEnabled() {
        return queryExpirationCheckEnabled;
    }

    @Override
    public String toString() {
        return "MapContainer{"
                + "name='" + name + '\''
                + ", destroyed=" + destroyed
                + '}';
    }
}
