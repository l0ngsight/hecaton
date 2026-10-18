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

package uk.org.hecaton.cardinality.impl;

import uk.org.hecaton.cardinality.impl.hyperloglog.HyperLogLog;
import uk.org.hecaton.cardinality.impl.operations.MergeOperation;
import uk.org.hecaton.cardinality.impl.operations.ReplicationOperation;
import uk.org.hecaton.config.CardinalityEstimatorConfig;
import uk.org.hecaton.internal.services.ManagedService;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.internal.services.SplitBrainHandlerService;
import uk.org.hecaton.internal.services.SplitBrainProtectionAwareService;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.merge.AbstractContainerMerger;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.CardinalityEstimatorMergeTypes;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.partition.MigrationAwareService;
import uk.org.hecaton.internal.partition.MigrationEndpoint;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.partition.PartitionReplicationEvent;
import uk.org.hecaton.internal.util.ConstructorFunction;
import uk.org.hecaton.internal.util.ContextMutexFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.partition.strategy.StringPartitioningStrategy.getPartitionKey;
import static uk.org.hecaton.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static uk.org.hecaton.internal.util.ConcurrencyUtil.getOrPutSynchronized;
import static uk.org.hecaton.internal.util.MapUtil.createHashMap;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

public class CardinalityEstimatorService
        implements ManagedService, RemoteService, MigrationAwareService, SplitBrainProtectionAwareService,
        SplitBrainHandlerService {

    public static final String SERVICE_NAME = "hz:impl:cardinalityEstimatorService";

    /**
     * Speculative factor to be used when initialising collections
     * of an approximate final size.
     */
    private static final double SIZING_FUDGE_FACTOR = 1.3;

    private static final Object NULL_OBJECT = new Object();

    private NodeEngine nodeEngine;
    private final ConcurrentMap<String, CardinalityEstimatorContainer> containers =
            new ConcurrentHashMap<>();
    private final ConstructorFunction<String, CardinalityEstimatorContainer> cardinalityEstimatorContainerConstructorFunction =
            new ConstructorFunction<>() {
                @Override
                public CardinalityEstimatorContainer createNew(String name) {
                    CardinalityEstimatorConfig config = nodeEngine.getConfig().findCardinalityEstimatorConfig(name);
                    return new CardinalityEstimatorContainer(config.getBackupCount(), config.getAsyncBackupCount());
                }
            };

    private final ConcurrentMap<String, Object> splitBrainProtectionConfigCache = new ConcurrentHashMap<>();
    private final ContextMutexFactory splitBrainProtectionConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> splitBrainProtectionConfigConstructor =
            new ConstructorFunction<>() {
        @Override
        public Object createNew(String name) {
            CardinalityEstimatorConfig config = nodeEngine.getConfig().findCardinalityEstimatorConfig(name);
            String splitBrainProtectionName = config.getSplitBrainProtectionName();
            return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
        }
    };

    public void addCardinalityEstimator(String name, CardinalityEstimatorContainer container) {
        checkNotNull(name, "Name can't be null");
        checkNotNull(container, "Container can't be null");

        containers.put(name, container);
    }

    public CardinalityEstimatorContainer getCardinalityEstimatorContainer(String name) {
        return getOrPutIfAbsent(containers, name, cardinalityEstimatorContainerConstructorFunction);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void reset() {
        containers.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public CardinalityEstimatorProxy createDistributedObject(String objectName, UUID source, boolean local) {
        return new CardinalityEstimatorProxy(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName, boolean local) {
        containers.remove(objectName);
        splitBrainProtectionConfigCache.remove(objectName);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        final IPartitionService partitionService = nodeEngine.getPartitionService();
        final int roughSize = (int) ((containers.size() * SIZING_FUDGE_FACTOR) / partitionService.getPartitionCount());
        Map<String, CardinalityEstimatorContainer> data = createHashMap(roughSize);
        int partitionId = event.getPartitionId();
        for (Map.Entry<String, CardinalityEstimatorContainer> containerEntry : containers.entrySet()) {
            String name = containerEntry.getKey();
            CardinalityEstimatorContainer container = containerEntry.getValue();

            if (partitionId == getPartitionId(name) && event.getReplicaIndex() <= container.getTotalBackupCount()) {
                data.put(name, containerEntry.getValue());
            }
        }

        return data.isEmpty() ? null : new ReplicationOperation(data);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearPartitionReplica(event.getPartitionId(), event.getNewReplicaIndex());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartitionReplica(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearPartitionReplica(int partitionId, int durabilityThreshold) {
        final Iterator<Map.Entry<String, CardinalityEstimatorContainer>> iterator = containers.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, CardinalityEstimatorContainer> entry = iterator.next();

            if (getPartitionId(entry.getKey()) == partitionId
                    && (durabilityThreshold == -1 || durabilityThreshold > entry.getValue().getTotalBackupCount())) {
                iterator.remove();
            }
        }
    }

    private int getPartitionId(String name) {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        String partitionKey = getPartitionKey(name);
        return partitionService.getPartitionId(partitionKey);
    }

    @Override
    public String getSplitBrainProtectionName(String name) {
        Object splitBrainProtectionName = getOrPutSynchronized(splitBrainProtectionConfigCache, name,
                splitBrainProtectionConfigCacheMutexFactory, splitBrainProtectionConfigConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

    @Override
    public Runnable prepareMergeRunnable() {
        CardinalityEstimatorContainerCollector collector = new CardinalityEstimatorContainerCollector(nodeEngine, containers);
        collector.run();
        return new Merger(collector);
    }

    private class Merger
            extends AbstractContainerMerger<CardinalityEstimatorContainer, HyperLogLog, CardinalityEstimatorMergeTypes> {

        Merger(CardinalityEstimatorContainerCollector collector) {
            super(collector, nodeEngine);
        }

        @Override
        protected String getLabel() {
            return "cardinality estimator";
        }

        @Override
        public void runInternal() {
            CardinalityEstimatorContainerCollector collector = (CardinalityEstimatorContainerCollector) this.collector;
            Map<Integer, Collection<CardinalityEstimatorContainer>> containerMap = collector.getCollectedContainers();
            for (Map.Entry<Integer, Collection<CardinalityEstimatorContainer>> entry : containerMap.entrySet()) {
                // TODO: batching support (tkountis)
                int partitionId = entry.getKey();
                Collection<CardinalityEstimatorContainer> containerList = entry.getValue();

                for (CardinalityEstimatorContainer container : containerList) {
                    String containerName = collector.getContainerName(container);
                    SplitBrainMergePolicy<HyperLogLog, CardinalityEstimatorMergeTypes, HyperLogLog> mergePolicy
                            = getMergePolicy(
                            collector.getMergePolicyConfig(container),
                            collector.getUserNamespaceContainer(container)
                    );

                    MergeOperation operation = new MergeOperation(containerName, mergePolicy, container.hll);
                    invoke(SERVICE_NAME, operation, partitionId);
                }
            }
        }
    }
}
