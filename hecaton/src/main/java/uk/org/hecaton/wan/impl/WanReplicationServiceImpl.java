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

package uk.org.hecaton.wan.impl;

import uk.org.hecaton.auditlog.AuditlogTypeIds;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.config.AbstractWanPublisherConfig;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.WanBatchPublisherConfig;
import uk.org.hecaton.config.WanConsumerConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.ManagedContext;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.management.events.WanConsistencyCheckIgnoredEvent;
import uk.org.hecaton.internal.management.events.WanSyncIgnoredEvent;
import uk.org.hecaton.internal.monitor.LocalWanStats;
import uk.org.hecaton.internal.monitor.WanSyncState;
import uk.org.hecaton.internal.monitor.impl.LocalWanStatsImpl;
import uk.org.hecaton.internal.partition.ChunkedMigrationAwareService;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.partition.PartitionReplicationEvent;
import uk.org.hecaton.internal.services.ManagedService;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.internal.services.WanSupportingService;
import uk.org.hecaton.internal.util.ConstructorFunction;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.version.Version;
import uk.org.hecaton.wan.WanConsumer;
import uk.org.hecaton.wan.WanEventCounters;
import uk.org.hecaton.wan.WanMigrationAwarePublisher;
import uk.org.hecaton.wan.WanPublisher;
import uk.org.hecaton.wan.impl.batch.WanBatchPublisher;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import static uk.org.hecaton.internal.nio.ClassLoaderUtil.getOrCreate;
import static uk.org.hecaton.internal.util.ConcurrencyUtil.getOrPutSynchronized;
import static uk.org.hecaton.internal.util.MapUtil.createConcurrentHashMap;
import static uk.org.hecaton.internal.util.MapUtil.createHashMap;
import static uk.org.hecaton.internal.util.StringUtil.isNullOrEmptyAfterTrim;

/**
 * Open source implementation of the {@link WanReplicationService}
 */
@SuppressWarnings("checkstyle:methodcount")
public class WanReplicationServiceImpl implements WanReplicationService,
        ChunkedMigrationAwareService, ManagedService {

    /**
     * Class names that select the built-in batch publisher: the default of {@link WanBatchPublisherConfig}
     * (the name of Hazelcast's Enterprise publisher, as renamed) and the Hecaton class.
     */
    private static final Set<String> BUILT_IN_BATCH_PUBLISHER_CLASS_NAMES = Set.of(
            "uk.org.hecaton.enterprise.wan.impl.replication.WanBatchPublisher", WanBatchPublisher.class.getName());

    private final Node node;

    /**
     * WAN event counters for all services and only received events
     */
    private final WanEventCounterRegistry receivedWanEventCounters = new WanEventCounterRegistry();

    /**
     * WAN event counters for all services and only sent events
     */
    private final WanEventCounterRegistry sentWanEventCounters = new WanEventCounterRegistry();

    private final ConcurrentMap<String, DelegatingWanScheme> wanReplications = createConcurrentHashMap(1);

    private final ConstructorFunction<String, DelegatingWanScheme> publisherDelegateConstructor;
    /** WAN replication configs added at runtime. */
    private final ConcurrentMap<String, WanReplicationConfig> addedConfigs = createConcurrentHashMap(1);
    /** Custom WAN consumers, by WAN replication name; created once the member has started. */
    private final Map<String, WanConsumer> consumers = new HashMap<>();
    private boolean started;

    public WanReplicationServiceImpl(Node node) {
        this.node = node;
        this.publisherDelegateConstructor = name -> {
            final WanReplicationConfig wanReplicationConfig = wanReplicationConfig(name);
            if (wanReplicationConfig == null) {
                return null;
            }
            return new DelegatingWanScheme(name, createPublishers(wanReplicationConfig));
        };
    }

    @Override
    public boolean hasWanReplicationScheme(String wanReplicationScheme) {
       return wanReplications.containsKey(wanReplicationScheme) || wanReplicationConfig(wanReplicationScheme) != null;
    }

    private WanReplicationConfig wanReplicationConfig(String name) {
        WanReplicationConfig added = addedConfigs.get(name);
        return added != null ? added : node.getConfig().getWanReplicationConfig(name);
    }


    @Override
    public DelegatingWanScheme getWanReplicationPublishers(String wanReplicationScheme) {
        return getOrPutSynchronized(wanReplications, wanReplicationScheme, this, publisherDelegateConstructor);
    }

    private ConcurrentMap<String, WanPublisher> createPublishers(WanReplicationConfig wanReplicationConfig) {
        List<AbstractWanPublisherConfig> allPublisherConfigs = new ArrayList<>(wanReplicationConfig.getBatchPublisherConfigs());
        allPublisherConfigs.addAll(wanReplicationConfig.getCustomPublisherConfigs());
        int publisherCount = allPublisherConfigs.size();

        if (publisherCount == 0) {
            return createConcurrentHashMap(1);
        }

        ConcurrentMap<String, WanPublisher> publishers = createConcurrentHashMap(publisherCount);
        Map<String, AbstractWanPublisherConfig> publisherConfigs = createHashMap(publisherCount);

        allPublisherConfigs.forEach(
                publisherConfig -> {
                    String publisherId = getWanPublisherId(publisherConfig);
                    if (publishers.containsKey(publisherId)) {
                        throw new InvalidConfigurationException(
                                "Detected duplicate publisher ID '" + publisherId + "' for a single WAN replication config");
                    }

                    WanPublisher publisher = createPublisher(publisherConfig);
                    publishers.put(publisherId, publisher);
                    publisherConfigs.put(publisherId, publisherConfig);
                });

        for (Entry<String, WanPublisher> publisherEntry : publishers.entrySet()) {
            String publisherId = publisherEntry.getKey();
            WanPublisher publisher = publisherEntry.getValue();
            ManagedContext managedContext = node.getSerializationService().getManagedContext();
            publisher = (WanPublisher) managedContext.initialize(publisher);
            publisher.init(wanReplicationConfig, publisherConfigs.get(publisherId));
        }

        return publishers;
    }

    /**
     * Instantiates a {@link WanPublisher} from the provided publisher
     * configuration.
     *
     * @param publisherConfig the WAN publisher configuration
     * @return the WAN replication publisher
     * @throws InvalidConfigurationException if the method was unable to create the publisher because there was no
     *                                       implementation or class name defined on the config
     */
    private WanPublisher createPublisher(AbstractWanPublisherConfig publisherConfig) {
        if (publisherConfig instanceof WanBatchPublisherConfig && publisherConfig.getImplementation() == null
                && (publisherConfig.getClassName() == null
                || BUILT_IN_BATCH_PUBLISHER_CLASS_NAMES.contains(publisherConfig.getClassName()))) {
            return new WanBatchPublisher(node);
        }
        WanPublisher publisher = getOrCreate(
                publisherConfig.getImplementation(),
                node.getConfigClassLoader(),
                publisherConfig.getClassName());
        if (publisher == null) {
            throw new InvalidConfigurationException("Either 'implementation' or 'className' "
                    + "attribute need to be set in the WAN publisher configuration for publisher " + publisherConfig);
        }
        return publisher;
    }

    /**
     * Returns the publisher ID for the given WAN publisher configuration which
     * is then used for identifying the WAN publisher in a WAN replication
     * scheme.
     * If the publisher ID is empty, returns the publisher group name.
     *
     * @param publisherConfig the WAN replication publisher configuration
     * @return the WAN publisher ID
     */
    public static @Nonnull
    String getWanPublisherId(AbstractWanPublisherConfig publisherConfig) {
        String publisherId = null;
        if (!isNullOrEmptyAfterTrim(publisherConfig.getPublisherId())) {
            publisherId = publisherConfig.getPublisherId();
        } else if (publisherConfig instanceof WanBatchPublisherConfig config) {
            publisherId = config.getClusterName();
        }
        if (publisherId == null) {
            throw new InvalidConfigurationException("Publisher ID or group name is not specified for " + publisherConfig);
        }
        return publisherId;
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            for (DelegatingWanScheme delegate : wanReplications.values()) {
                for (WanPublisher publisher : delegate.getPublishers()) {
                    if (publisher != null) {
                        publisher.shutdown();
                    }
                }
            }
            wanReplications.clear();
            consumers.values().forEach(WanConsumer::shutdown);
            consumers.clear();
        }
    }

    /**
     * Called once the member has started: creates the custom WAN consumers of the WAN replication
     * configs.
     */
    public synchronized void afterStart() {
        started = true;
        node.getNodeEngine().getMetricsRegistry().registerDynamicMetricsProvider(new WanMetricsProvider(wanReplications));
        node.getConfig().getWanReplicationConfigs().values().forEach(this::startConsumer);
        addedConfigs.values().forEach(this::startConsumer);
    }

    private void startConsumer(WanReplicationConfig config) {
        WanConsumerConfig consumerConfig = config.getConsumerConfig();
        if (consumerConfig == null || consumers.containsKey(config.getName())
                || consumerConfig.getImplementation() == null && consumerConfig.getClassName() == null) {
            return;
        }
        WanConsumer consumer;
        try {
            consumer = getOrCreate(consumerConfig.getImplementation(), node.getConfigClassLoader(),
                    consumerConfig.getClassName());
        } catch (Exception e) {
            throw new InvalidConfigurationException("Cannot create the WAN consumer " + consumerConfig.getClassName(), e);
        }
        consumer = (WanConsumer) node.getSerializationService().getManagedContext().initialize(consumer);
        consumer.init(config.getName(), consumerConfig);
        consumers.put(config.getName(), consumer);
    }

    @Override
    public void pause(String wanReplicationName, String wanPublisherId) {
        internalPublisher(wanReplicationName, wanPublisherId).pause();
    }

    @Override
    public void stop(String wanReplicationName, String wanPublisherId) {
        internalPublisher(wanReplicationName, wanPublisherId).stop();
    }

    @Override
    public void resume(String wanReplicationName, String wanPublisherId) {
        internalPublisher(wanReplicationName, wanPublisherId).resume();
    }

    private InternalWanPublisher<?> internalPublisher(String wanReplicationName, String wanPublisherId) {
        WanPublisher<?> publisher = getPublisherOrFail(wanReplicationName, wanPublisherId);
        if (publisher instanceof InternalWanPublisher<?> internal) {
            return internal;
        }
        throw new UnsupportedOperationException("WAN publisher " + wanPublisherId + " of " + wanReplicationName
                + " is a custom publisher, which manages its own state");
    }

    @Override
    public UUID syncMap(String wanReplicationName, String wanPublisherId, String mapName) {
        node.getManagementCenterService().log(
                WanSyncIgnoredEvent.enterpriseOnly(wanReplicationName, wanPublisherId, mapName));
        throw new UnsupportedOperationException("WAN sync for map is not supported.");
    }

    @Override
    public UUID syncAllMaps(String wanReplicationName, String wanPublisherId) {
        node.getManagementCenterService().log(
                WanSyncIgnoredEvent.enterpriseOnly(wanReplicationName, wanPublisherId, null));
        throw new UnsupportedOperationException("WAN sync is not supported.");
    }

    @Override
    public WanSyncStateResult getSyncResult(UUID syncUUID) {
        throw new UnsupportedOperationException("WAN sync is not supported.");
    }

    @Override
    public UUID consistencyCheck(String wanReplicationName, String wanPublisherId, String mapName) {
        node.getManagementCenterService().log(
                WanConsistencyCheckIgnoredEvent.enterpriseOnly(wanReplicationName, wanPublisherId, mapName));

        throw new UnsupportedOperationException("Consistency check is not supported.");
    }

    @Override
    public void removeWanEvents(String wanReplicationName, String wanPublisherId) {
        internalPublisher(wanReplicationName, wanPublisherId).removeWanEvents();
    }

    @Override
    public AddWanConfigResult addWanReplicationConfig(WanReplicationConfig wanReplicationConfig) {
        String name = wanReplicationConfig.getName();
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        boolean known = node.getConfig().getWanReplicationConfigs().containsKey(name)
                || nodeEngine.getConfigurationService().findWanReplicationConfig(name) != null;
        AddWanConfigResult result = addConfig(wanReplicationConfig);
        node.getNodeExtension().getAuditlogService()
                .eventBuilder(AuditlogTypeIds.WAN_ADD_CONFIG)
                .message("WAN replication configuration added.")
                .addParameter("wanReplicationName", name)
                .addParameter("addedPublisherIds", result.getAddedPublisherIds())
                .log();
        if (!known) {
            // a new scheme is kept in the dynamic configuration, so members that join later get it too;
            // its members add it locally
            nodeEngine.getConfigurationService().broadcastConfig(wanReplicationConfig);
            return result;
        }
        List<InternalCompletableFuture<Object>> futures = new ArrayList<>();
        for (Member member : nodeEngine.getClusterService().getMembers()) {
            if (!member.localMember()) {
                futures.add(nodeEngine.getOperationService().invokeOnTarget(SERVICE_NAME,
                        new AddWanConfigOperation(wanReplicationConfig), member.getAddress()));
            }
        }
        futures.forEach(InternalCompletableFuture::joinInternal);
        return result;
    }

    @Override
    public void addWanReplicationConfigLocally(WanReplicationConfig wanReplicationConfig) {
        addConfig(wanReplicationConfig);
    }

    /**
     * Adds a WAN replication config: a new scheme, or new publishers of an existing one (publishers
     * that exist already are left as they are).
     */
    private synchronized AddWanConfigResult addConfig(WanReplicationConfig newConfig) {
        String name = newConfig.getName();
        if (!hasWanReplicationScheme(name)) {
            addedConfigs.put(name, newConfig);
            if (started) {
                startConsumer(newConfig);
            }
        }
        DelegatingWanScheme scheme = getWanReplicationPublishers(name);
        List<AbstractWanPublisherConfig> publisherConfigs = new ArrayList<>(newConfig.getBatchPublisherConfigs());
        publisherConfigs.addAll(newConfig.getCustomPublisherConfigs());
        Set<String> added = new HashSet<>();
        Set<String> ignored = new HashSet<>();
        for (AbstractWanPublisherConfig publisherConfig : publisherConfigs) {
            String publisherId = getWanPublisherId(publisherConfig);
            if (scheme.getPublisher(publisherId) != null) {
                ignored.add(publisherId);
                continue;
            }
            WanPublisher publisher = createPublisher(publisherConfig);
            publisher = (WanPublisher) node.getSerializationService().getManagedContext().initialize(publisher);
            publisher.init(newConfig, publisherConfig);
            scheme.addPublisher(publisherId, publisher);
            added.add(publisherId);
        }
        if (!added.isEmpty()) {
            // structures referring to the scheme start publishing to it
            node.getNodeEngine().getServices(WanSupportingService.class).forEach(WanSupportingService::onWanConfigChange);
        }
        return new AddWanConfigResult(added, ignored);
    }

    @Override
    public Map<String, LocalWanStats> getStats() {
        Map<String, LocalWanStats> stats = createHashMap(wanReplications.size());
        for (Entry<String, DelegatingWanScheme> scheme : wanReplications.entrySet()) {
            LocalWanStatsImpl schemeStats = new LocalWanStatsImpl();
            schemeStats.setLocalPublisherStatsMap(scheme.getValue().getStats());
            stats.put(scheme.getKey(), schemeStats);
        }
        return stats;
    }

    @Override
    public WanSyncState getWanSyncState() {
        return null;
    }

    @Override
    public WanEventCounters getReceivedEventCounters(String serviceName) {
        return receivedWanEventCounters.getWanEventCounter("", "", serviceName);
    }

    @Override
    public WanEventCounters getSentEventCounters(String wanReplicationName,
                                                 String wanPublisherId,
                                                 String serviceName) {
        return sentWanEventCounters.getWanEventCounter(wanReplicationName, wanPublisherId, serviceName);
    }

    @Override
    public void removeWanEventCounters(String serviceName, String objectName) {
        receivedWanEventCounters.removeCounter(serviceName, objectName);
        sentWanEventCounters.removeCounter(serviceName, objectName);
    }

    @Override
    public List<Version> getSupportedWanProtocolVersions() {
        // EE WAN replication not supported in OS
        return Collections.emptyList();
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        notifyMigrationAwarePublishers(p -> p.onMigrationStart(event));
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        notifyMigrationAwarePublishers(p -> p.onMigrationCommit(event));
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        notifyMigrationAwarePublishers(p -> p.onMigrationRollback(event));
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        // NOP
    }

    @Override
    public void reset() {
        final Collection<DelegatingWanScheme> publishers = wanReplications.values();
        for (DelegatingWanScheme publisherDelegate : publishers) {
            for (WanPublisher publisher : publisherDelegate.getPublishers()) {
                publisher.reset();
            }
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        if (wanReplications.isEmpty()) {
            return Collections.emptyList();
        }

        Set<ServiceNamespace> namespaces = new HashSet<>();
        for (DelegatingWanScheme publisher : wanReplications.values()) {
            publisher.collectAllServiceNamespaces(event, namespaces);
        }
        return namespaces;
    }

    /**
     * {@inheritDoc}
     * WAN events are emitted for maps and caches.
     */
    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        final String serviceName = namespace.getServiceName();
        return namespace instanceof ObjectNamespace
                && (MapService.SERVICE_NAME.equals(serviceName) || ICacheService.SERVICE_NAME.equals(serviceName));
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        if (wanReplications.isEmpty() || namespaces.isEmpty()) {
            return null;
        }
        Map<String, Map<String, Object>> eventContainers = createHashMap(wanReplications.size());

        for (Entry<String, DelegatingWanScheme> wanReplicationEntry : wanReplications.entrySet()) {
            String replicationScheme = wanReplicationEntry.getKey();
            DelegatingWanScheme delegate = wanReplicationEntry.getValue();
            Map<String, Object> publisherEventContainers = delegate.prepareEventContainerReplicationData(event, namespaces);
            if (!publisherEventContainers.isEmpty()) {
                eventContainers.put(replicationScheme, publisherEventContainers);
            }
        }

        if (eventContainers.isEmpty()) {
            return null;
        } else {
            return new WanEventContainerReplicationOperation(
                    Collections.emptyList(), eventContainers, event.getPartitionId(), event.getReplicaIndex());
        }
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return prepareReplicationOperation(event, getAllServiceNamespaces(event));
    }

    @Override
    public WanPublisher getPublisherOrFail(String wanReplicationName,
                                           String wanPublisherId) {
        WanPublisher publisher = getPublisherOrNull(wanReplicationName, wanPublisherId);
        if (publisher == null) {
            throw new InvalidConfigurationException("WAN Replication Config doesn't exist with WAN configuration name "
                    + wanReplicationName + " and publisher ID " + wanPublisherId);
        }
        return publisher;
    }

    @Override
    public boolean appendWanReplicationConfig(WanReplicationConfig newConfig) {
        return !addConfig(newConfig).getAddedPublisherIds().isEmpty();
    }

    private WanPublisher getPublisherOrNull(String wanReplicationName,
                                            String wanPublisherId) {
        if (!hasWanReplicationScheme(wanReplicationName)) {
            return null;
        }

        return getWanReplicationPublishers(wanReplicationName).getPublisher(wanPublisherId);
    }

    private void notifyMigrationAwarePublishers(Consumer<WanMigrationAwarePublisher> publisherConsumer) {
        for (DelegatingWanScheme wanReplication : wanReplications.values()) {
            for (WanPublisher publisher : wanReplication.getPublishers()) {
                if (publisher instanceof WanMigrationAwarePublisher awarePublisher) {
                    publisherConsumer.accept(awarePublisher);
                }
            }
        }
    }
}
