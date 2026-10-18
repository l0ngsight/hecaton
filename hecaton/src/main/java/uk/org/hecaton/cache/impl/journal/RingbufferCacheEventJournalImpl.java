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

package uk.org.hecaton.cache.impl.journal;

import uk.org.hecaton.cache.CacheEventType;
import uk.org.hecaton.cache.CacheNotExistsException;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.EventJournalConfig;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.DataType;
import uk.org.hecaton.ringbuffer.impl.ReadResultSetImpl;
import uk.org.hecaton.ringbuffer.impl.RingbufferContainer;
import uk.org.hecaton.ringbuffer.impl.RingbufferService;
import uk.org.hecaton.ringbuffer.impl.RingbufferWaitNotifyKey;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationparker.OperationParker;
import uk.org.hecaton.spi.impl.operationservice.WaitNotifyKey;

import static uk.org.hecaton.cache.CacheEventType.CREATED;
import static uk.org.hecaton.cache.CacheEventType.EVICTED;
import static uk.org.hecaton.cache.CacheEventType.EXPIRED;
import static uk.org.hecaton.cache.CacheEventType.REMOVED;
import static uk.org.hecaton.cache.CacheEventType.UPDATED;
import static java.lang.String.format;


/**
 * The cache event journal implementation based on the {@link uk.org.hecaton.ringbuffer.Ringbuffer}.
 * It will add all journal events into a {@link RingbufferContainer} with the provided namespace
 * and partition ID and allows checking if the cache has a configured event journal.
 * Adapts the {@link EventJournalConfig} to the {@link RingbufferConfig} when creating the ringbuffer.
 */
public class RingbufferCacheEventJournalImpl implements CacheEventJournal {

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    public RingbufferCacheEventJournalImpl(NodeEngine engine) {
        this.nodeEngine = (NodeEngineImpl) engine;
        this.logger = this.nodeEngine.getLogger(RingbufferCacheEventJournalImpl.class);
    }

    @Override
    public void writeUpdateEvent(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId,
                                 Data key, Object oldValue, Object newValue) {
        addToEventRingbuffer(journalConfig, namespace, partitionId, UPDATED, key, oldValue, newValue);
    }

    @Override
    public void writeCreatedEvent(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId,
                                  Data key, Object value) {
        addToEventRingbuffer(journalConfig, namespace, partitionId, CREATED, key, null, value);
    }

    @Override
    public void writeRemoveEvent(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId,
                                 Data key, Object value) {
        addToEventRingbuffer(journalConfig, namespace, partitionId, REMOVED, key, value, null);
    }

    @Override
    public void writeEvictEvent(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId,
                                Data key, Object value) {
        addToEventRingbuffer(journalConfig, namespace, partitionId, EVICTED, key, value, null);
    }

    @Override
    public void writeExpiredEvent(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId,
                                  Data key, Object value) {
        addToEventRingbuffer(journalConfig, namespace, partitionId, EXPIRED, key, value, null);
    }

    @Override
    public long newestSequence(ObjectNamespace namespace, int partitionId) {
        return getRingbufferOrFail(namespace, partitionId).tailSequence();
    }

    @Override
    public long oldestSequence(ObjectNamespace namespace, int partitionId) {
        return getRingbufferOrFail(namespace, partitionId).headSequence();
    }

    @Override
    public boolean isPersistenceEnabled(ObjectNamespace namespace, int partitionId) {
        return getRingbufferOrFail(namespace, partitionId).getStore().isEnabled();
    }

    @Override
    public void destroy(ObjectNamespace namespace, int partitionId) {
        RingbufferService service;
        try {
            service = getRingbufferService();
        } catch (Exception e) {
            if (nodeEngine.isRunning()) {
                logger.fine("Could not retrieve ringbuffer service to destroy event journal " + namespace, e);
            }
            return;
        }
        service.destroyContainer(partitionId, namespace);
    }

    @Override
    public void isAvailableOrNextSequence(ObjectNamespace namespace, int partitionId, long sequence) {
        getRingbufferOrFail(namespace, partitionId).checkBlockableReadSequence(sequence);
    }

    @Override
    public boolean isNextAvailableSequence(ObjectNamespace namespace, int partitionId, long sequence) {
        return getRingbufferOrFail(namespace, partitionId).shouldWait(sequence);
    }

    @Override
    public WaitNotifyKey getWaitNotifyKey(ObjectNamespace namespace, int partitionId) {
        return new RingbufferWaitNotifyKey(namespace, partitionId);
    }

    @Override
    public <T> long readMany(ObjectNamespace namespace, int partitionId, long beginSequence,
                             ReadResultSetImpl<InternalEventJournalCacheEvent, T> resultSet) {
        return getRingbufferOrFail(namespace, partitionId).readMany(beginSequence, resultSet);
    }

    @Override
    public void cleanup(ObjectNamespace namespace, int partitionId) {
        getRingbufferOrFail(namespace, partitionId).cleanup();
    }

    /**
     * {@inheritDoc}
     * NOTE: The cache config should have already been created in the cache service before
     * invoking this method.
     *
     * @param namespace the cache namespace, containing the full prefixed cache name
     * @return {@code true} if the object has a configured and enabled event journal, {@code false} otherwise
     * @throws CacheNotExistsException if the cache configuration was not found
     */
    @Override
    public boolean hasEventJournal(ObjectNamespace namespace) {
        return getEventJournalConfig(namespace) != null;
    }

    /**
     * {@inheritDoc}
     *
     * @throws CacheNotExistsException if the cache configuration was not found
     */
    @Override
    public EventJournalConfig getEventJournalConfig(ObjectNamespace namespace) {
        String name = namespace.getObjectName();
        CacheConfig cacheConfig = getCacheService().getCacheConfig(name);
        if (cacheConfig == null) {
            throw new CacheNotExistsException("Cache " + name + " is already destroyed or not created yet, on "
                    + nodeEngine.getLocalMember());
        }
        EventJournalConfig config = cacheConfig.getEventJournalConfig();
        if (config == null || !config.isEnabled()) {
            return null;
        }
        return config;
    }

    /**
     * {@inheritDoc}
     *
     * @throws CacheNotExistsException if the cache configuration was not found
     */
    @Override
    public RingbufferConfig toRingbufferConfig(EventJournalConfig config, ObjectNamespace namespace) {
        CacheConfig cacheConfig = getCacheService().getCacheConfig(namespace.getObjectName());
        if (cacheConfig == null) {
            throw new CacheNotExistsException("Cache " + namespace.getObjectName()
                    + " is already destroyed or not created yet, on "
                    + nodeEngine.getLocalMember());
        }
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        return new RingbufferConfig()
                .setAsyncBackupCount(cacheConfig.getAsyncBackupCount())
                .setBackupCount(cacheConfig.getBackupCount())
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setCapacity(config.getCapacity() / partitionCount)
                .setTimeToLiveSeconds(config.getTimeToLiveSeconds());
    }

    private void addToEventRingbuffer(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId,
                                      CacheEventType eventType, Data key, Object oldValue, Object newValue) {
        if (journalConfig == null || !journalConfig.isEnabled()) {
            return;
        }
        RingbufferContainer<InternalEventJournalCacheEvent, Object> eventContainer
                = getRingbufferOrNull(journalConfig, namespace, partitionId);
        if (eventContainer == null) {
            return;
        }
        eventContainer.maybeCleanup();
        InternalEventJournalCacheEvent event
                = new InternalEventJournalCacheEvent(toData(key), toData(newValue), toData(oldValue), eventType.getType());
        eventContainer.add(event);
        getOperationParker().unpark(eventContainer);
    }

    protected Data toData(Object val) {
        return getSerializationService().toData(val, DataType.HEAP);
    }

    /**
     * Gets or creates a ringbuffer for an event journal or throws an exception if no
     * event journal is configured. The caller should call
     * {@link #hasEventJournal(ObjectNamespace)} before invoking this method to avoid getting
     * the exception. This method can be used to get the ringbuffer on which future events
     * will be added once the cache has been created.
     * <p>
     * NOTE: The cache config should have already been created in the cache service before
     * invoking this method.
     *
     * @param namespace   the cache namespace, containing the full prefixed cache name
     * @param partitionId the cache partition ID
     * @return the cache partition event journal
     * @throws CacheNotExistsException if the cache configuration was not found
     * @throws IllegalStateException   if there is no event journal configured for this cache
     */
    private RingbufferContainer<InternalEventJournalCacheEvent, Object> getRingbufferOrFail(ObjectNamespace namespace,
                                                                                            int partitionId) {
        RingbufferService ringbufferService = getRingbufferService();
        RingbufferContainer<InternalEventJournalCacheEvent, Object> container
                = ringbufferService.getContainerOrNull(partitionId, namespace);
        if (container != null) {
            return container;
        }

        EventJournalConfig config = getEventJournalConfig(namespace);
        if (config == null) {
            throw new IllegalStateException(format(
                    "There is no event journal configured for cache %s or the journal is disabled",
                    namespace.getObjectName()));
        }
        return getOrCreateRingbufferContainer(namespace, partitionId, config);
    }

    /**
     * Gets or creates a ringbuffer for an event journal or returns {@code null} if no
     * event journal is configured, it is disabled or not available.
     *
     * @param journalConfig the event journal configuration for this specific cache
     * @param namespace     the cache namespace, containing the full prefixed cache name
     * @param partitionId   the cache partition ID
     * @return the cache partition event journal or {@code null} if no journal is configured for this cache
     * @throws CacheNotExistsException if the cache configuration was not found
     * @see #getEventJournalConfig(ObjectNamespace)
     */
    private RingbufferContainer<InternalEventJournalCacheEvent, Object> getRingbufferOrNull(
            EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId) {
        RingbufferService ringbufferService = getRingbufferService();
        RingbufferContainer<InternalEventJournalCacheEvent, Object> container
                = ringbufferService.getContainerOrNull(partitionId, namespace);
        if (container != null) {
            return container;
        }

        return journalConfig != null ? getOrCreateRingbufferContainer(namespace, partitionId, journalConfig) : null;
    }

    private RingbufferContainer<InternalEventJournalCacheEvent, Object> getOrCreateRingbufferContainer(
            ObjectNamespace namespace, int partitionId, EventJournalConfig config) {
        RingbufferConfig ringbufferConfig = toRingbufferConfig(config, namespace);
        return getRingbufferService().getOrCreateContainer(partitionId, namespace, ringbufferConfig);
    }

    private RingbufferService getRingbufferService() {
        return nodeEngine.getService(RingbufferService.SERVICE_NAME);
    }

    private OperationParker getOperationParker() {
        return nodeEngine.getOperationParker();
    }

    private InternalSerializationService getSerializationService() {
        return (InternalSerializationService) nodeEngine.getSerializationService();
    }

    private CacheService getCacheService() {
        return nodeEngine.getService(CacheService.SERVICE_NAME);
    }
}
