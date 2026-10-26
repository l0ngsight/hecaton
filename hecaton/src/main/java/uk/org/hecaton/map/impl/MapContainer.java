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

import uk.org.hecaton.config.EventJournalConfig;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.map.impl.eviction.Evictor;
import uk.org.hecaton.map.impl.mapstore.MapStoreContext;
import uk.org.hecaton.map.impl.wan.MapWanContext;
import uk.org.hecaton.partition.PartitioningStrategy;
import uk.org.hecaton.query.impl.IndexRegistry;
import uk.org.hecaton.query.impl.QueryableEntry;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.properties.HecatonProperty;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public interface MapContainer {

    String PROP_QUERY_EXPIRATION_CHECK_ENABLED
            = "hecaton.internal.map.query.expiration.check.enabled";

    boolean DEFAULT_QUERY_EXPIRATION_CHECK_ENABLED = true;

    HecatonProperty QUERY_EXPIRATION_CHECK_ENABLED
            = new HecatonProperty(PROP_QUERY_EXPIRATION_CHECK_ENABLED, DEFAULT_QUERY_EXPIRATION_CHECK_ENABLED);

    void init();

    /**
     * @param global      set {@code true} to create global indexes, otherwise set
     *                    {@code false} to have partitioned indexes
     * @param partitionId the partition ID the index is created on. {@code -1}
     *                    for global indexes.
     * @return a new IndexRegistry object
     */
    IndexRegistry createIndexRegistry(boolean global, int partitionId);

    AtomicLong getLastInvalidMergePolicyCheckTime();

    void initEvictor();

    boolean shouldUseGlobalIndex();

    /**
     * Used to get index registry of one
     * of global or partitioned indexes.
     *
     * @return by default always returns global-index
     * registry otherwise return partitioned-index registry
     */
    IndexRegistry getOrCreateIndexRegistry(int partitionId);

    /**
     * @return the global index, if the global index is in use or null.
     */
    IndexRegistry getGlobalIndexRegistry();

    @Nullable
    IndexRegistry getOrNullPartitionedIndexRegistry(int partitionId);

    // Only used for testing
    ConcurrentMap<Integer, IndexRegistry> getPartitionedIndexRegistry();

    // Only used for testing
    boolean isEmptyIndexRegistry();
    MapWanContext getWanContext();


    int getTotalBackupCount();

    int getBackupCount();

    int getAsyncBackupCount();

    PartitioningStrategy getPartitioningStrategy();

    MapServiceContext getMapServiceContext();

    MapStoreContext getMapStoreContext();

    MapConfig getMapConfig();

    /**
     * Whether the map is immutable: held values are never updated, and are shared by readers (load protection,
     * F23). True when load protection is on and the map is configured immutable.
     */
    default boolean isImmutable() {
        return getMapServiceContext().getNodeEngine() instanceof NodeEngineImpl nodeEngine
                && nodeEngine.getLoadProtection().isImmutable(getMapConfig());
    }

    void setMapConfig(MapConfig mapConfig);

    EventJournalConfig getEventJournalConfig();

    String getName();

    String getSplitBrainProtectionName();

    Function<Object, Data> toData();

    QueryableEntry newQueryEntry(Data key, Object value);

    Evictor getEvictor();

    // only used for testing purposes
    void setEvictor(Evictor evictor);

    Extractors getExtractors();

    boolean hasInvalidationListener();

    AtomicInteger getInvalidationListenerCounter();

    void increaseInvalidationListenerCount();

    void decreaseInvalidationListenerCount();

    InterceptorRegistry getInterceptorRegistry();

    /**
     * Callback invoked before record store and indexes are destroyed. Ensures that if map iterator observes a non-destroyed
     * state, then associated data structures are still valid.
     */
    void onBeforeDestroy();

    // callback called when the MapContainer is de-registered
    // from MapService and destroyed - basically on map-destroy
    void onDestroy();
    boolean isDestroyed();

    boolean shouldCloneOnEntryProcessing(int partitionId);

    ObjectNamespace getObjectNamespace();
    Map<String, IndexConfig> getIndexDefinitions();

    boolean isUseCachedDeserializedValuesEnabled(int partitionId);

}
