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

package uk.org.hecaton.collection.impl.collection;

import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.collection.ItemEvent;
import uk.org.hecaton.collection.ItemListener;
import uk.org.hecaton.collection.impl.collection.operations.CollectionMergeOperation;
import uk.org.hecaton.collection.impl.collection.operations.CollectionOperation;
import uk.org.hecaton.collection.impl.common.DataAwareItemEvent;
import uk.org.hecaton.collection.impl.txncollection.operations.CollectionTransactionRollbackOperation;
import uk.org.hecaton.core.ItemEventType;
import uk.org.hecaton.internal.monitor.impl.AbstractLocalCollectionStats;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.partition.MigrationAwareService;
import uk.org.hecaton.internal.partition.MigrationEndpoint;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.partition.PartitionReplicationEvent;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.services.ManagedService;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.internal.services.SplitBrainHandlerService;
import uk.org.hecaton.internal.services.SplitBrainProtectionAwareService;
import uk.org.hecaton.internal.services.TransactionalService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.partition.strategy.StringPartitioningStrategy;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.eventservice.EventPublishingService;
import uk.org.hecaton.spi.impl.merge.AbstractContainerMerger;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.CollectionMergeTypes;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.spi.impl.merge.MergingValueFactory.createMergingValue;

public abstract class CollectionService implements ManagedService, RemoteService, EventPublishingService<CollectionEvent,
        ItemListener<Data>>, TransactionalService, MigrationAwareService, SplitBrainProtectionAwareService,
        SplitBrainHandlerService {

    protected final NodeEngine nodeEngine;
    protected final SerializationService serializationService;
    protected final IPartitionService partitionService;

    private final ILogger logger;

    protected CollectionService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
        getContainerMap().clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public void destroyDistributedObject(String name, boolean local) {
        CollectionContainer container = getContainerMap().remove(name);
        if (container != null) {
            container.destroy();
        }
        nodeEngine.getEventService().deregisterAllLocalListeners(getServiceName(), name);
    }

    public abstract CollectionContainer getOrCreateContainer(String name, boolean backup);

    public abstract ConcurrentMap<String, ? extends CollectionContainer> getContainerMap();

    public abstract String getServiceName();

    @Override
    public void dispatchEvent(CollectionEvent event, ItemListener<Data> listener) {
        final MemberImpl member = nodeEngine.getClusterService().getMember(event.getCaller());
        ItemEvent<Data> itemEvent = new DataAwareItemEvent<>(event.getName(), event.getEventType(), event.getData(),
                member, serializationService);
        if (member == null) {
            if (logger.isInfoEnabled()) {
                logger.info("Dropping event " + itemEvent + " from unknown address:" + event.getCaller());
            }
            return;
        }

        String namespace = lookupNamespace(event.getName());
        NamespaceUtil.runWithNamespace(nodeEngine, namespace, () -> {
            if (event.getEventType() == ItemEventType.ADDED) {
                listener.itemAdded(itemEvent);
            } else {
                listener.itemRemoved(itemEvent);
            }
        });
    }

    @Override
    public void rollbackTransaction(UUID transactionId) {
        final Set<String> collectionNames = getContainerMap().keySet();
        OperationService operationService = nodeEngine.getOperationService();
        for (String name : collectionNames) {
            int partitionId = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            Operation operation = new CollectionTransactionRollbackOperation(name, transactionId)
                    .setPartitionId(partitionId)
                    .setService(this)
                    .setNodeEngine(nodeEngine);
            operationService.invokeOnPartition(operation);
        }
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    protected Map<String, CollectionContainer> getMigrationData(PartitionReplicationEvent event) {
        Map<String, CollectionContainer> migrationData = new HashMap<>();
        for (Map.Entry<String, ? extends CollectionContainer> entry : getContainerMap().entrySet()) {
            String name = entry.getKey();
            int partitionId = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            CollectionContainer container = entry.getValue();
            if (partitionId == event.getPartitionId() && container.getConfig().getTotalBackupCount() >= event.getReplicaIndex()) {
                migrationData.put(name, container);
            }
        }
        return migrationData;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearCollectionsHavingLesserBackupCountThan(event.getPartitionId(), event.getNewReplicaIndex());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearCollectionsHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearCollectionsHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        Set<? extends Map.Entry<String, ? extends CollectionContainer>> entrySet = getContainerMap().entrySet();
        Iterator<? extends Map.Entry<String, ? extends CollectionContainer>> iterator = entrySet.iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, ? extends CollectionContainer> entry = iterator.next();
            String name = entry.getKey();
            CollectionContainer container = entry.getValue();
            int containerPartitionId = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            if (containerPartitionId != partitionId) {
                continue;
            }

            if (thresholdReplicaIndex < 0 || thresholdReplicaIndex > container.getConfig().getTotalBackupCount()) {
                container.destroy();
                iterator.remove();
            }
        }
    }

    public void addContainer(String name, CollectionContainer container) {
        getRawContainerMap().put(name, container);
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<String, CollectionContainer> getRawContainerMap() {
        return (ConcurrentMap<String, CollectionContainer>) getContainerMap();
    }

    @Override
    public Runnable prepareMergeRunnable() {
        CollectionContainerCollector collector = new CollectionContainerCollector(nodeEngine, getRawContainerMap());
        collector.run();
        return new Merger(collector);
    }

    public abstract AbstractLocalCollectionStats getLocalCollectionStats(String name);

    private class Merger extends AbstractContainerMerger<CollectionContainer, Collection<Object>, CollectionMergeTypes<Object>> {

        Merger(CollectionContainerCollector collector) {
            super(collector, nodeEngine);
        }

        @Override
        protected String getLabel() {
            return "collection";
        }

        @Override
        public void runInternal() {
            for (Map.Entry<Integer, Collection<CollectionContainer>> entry : collector.getCollectedContainers().entrySet()) {
                int partitionId = entry.getKey();
                Collection<CollectionContainer> containerList = entry.getValue();
                for (CollectionContainer container : containerList) {
                    // TODO: add batching (which is a bit complex, since collections don't have a multi-name operation yet
                    Collection<CollectionItem> items = container.getCollection();

                    String name = container.getName();
                    var config = container.getConfig();
                    SplitBrainMergePolicy<Collection<Object>, CollectionMergeTypes<Object>, Collection<Object>> mergePolicy
                            = getMergePolicy(config.getMergePolicyConfig(), config.getUserCodeNamespace());

                    CollectionMergeTypes<Object> mergingValue = createMergingValue(serializationService, items);
                    sendBatch(partitionId, name, mergePolicy, mergingValue);

                    items.clear();
                }
            }
        }

        private void sendBatch(int partitionId, String name,
                               SplitBrainMergePolicy<Collection<Object>,
                                       CollectionMergeTypes<Object>, Collection<Object>> mergePolicy,
                               CollectionMergeTypes<Object> mergingValue) {
            CollectionOperation operation = new CollectionMergeOperation(name, mergePolicy, mergingValue);
            invoke(getServiceName(), operation, partitionId);
        }
    }

    public static String lookupNamespace(NodeEngine engine, String serviceName, String setName) {
        if (engine.getNamespaceService().isEnabled()) {
            CollectionService service = engine.getService(serviceName);
            return service.lookupNamespace(setName);
        }
        return null;
    }

    // For faster access within the service, primary calling points are from static
    //     methods in implementing services
    protected abstract String lookupNamespace(String collectionName);
}
