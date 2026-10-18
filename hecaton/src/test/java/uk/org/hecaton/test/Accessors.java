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
package uk.org.hecaton.test;

import uk.org.hecaton.auditlog.AuditlogService;
import uk.org.hecaton.client.impl.ClientEngineImpl;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.TestUtil;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.metrics.MetricsRegistry;
import uk.org.hecaton.internal.partition.IPartition;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.internal.partition.impl.PartitionReplicaStateChecker;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.server.ServerConnectionManager;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.query.impl.IndexRegistry;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.test.starter.HecatonStarter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static uk.org.hecaton.test.starter.ReflectionUtils.getFieldValueReflectively;

/**
 * Utility accessors for Hecaton internals
 */
public class Accessors {

    private Accessors() {
    }

    public static Node getNode(HecatonInstance hz) {
        return TestUtil.getNode(hz);
    }

    public static HecatonInstanceImpl getHecatonInstanceImpl(HecatonInstance hz) {
        return TestUtil.getHecatonInstanceImpl(hz);
    }

    public static NodeEngineImpl getNodeEngineImpl(HecatonInstance hz) {
        return getNode(hz).getNodeEngine();
    }

    public static ClientEngineImpl getClientEngineImpl(HecatonInstance instance) {
        return (ClientEngineImpl) getNode(instance).getClientEngine();
    }

    public static ServerConnectionManager getConnectionManager(HecatonInstance hz) {
        return getNode(hz).getServer().getConnectionManager(EndpointQualifier.MEMBER);
    }

    public static ClusterService getClusterService(HecatonInstance hz) {
        return getNode(hz).getClusterService();
    }

    public static InternalPartitionService getPartitionService(HecatonInstance hz) {
        return getNode(hz).getPartitionService();
    }

    public static boolean isPartitionStateInitialized(HecatonInstance hz) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(hz);
        return partitionService.getPartitionStateManager().isInitialized();
    }

    public static InternalSerializationService getSerializationService(HecatonInstance hz) {
        return getNode(hz).getSerializationService();
    }

    public static OperationServiceImpl getOperationService(HecatonInstance hz) {
        return getNodeEngineImpl(hz).getOperationService();
    }

    public static MetricsRegistry getMetricsRegistry(HecatonInstance hz) {
        return getNodeEngineImpl(hz).getMetricsRegistry();
    }

    public static AuditlogService getAuditlogService(HecatonInstance hz) {
        return getNode(hz).getNodeExtension().getAuditlogService();
    }

    public static <T> T getService(HecatonInstance hz, String serviceName) {
        return getNodeEngineImpl(hz).getService(serviceName);
    }

    public static Address getAddress(HecatonInstance hz) {
        return getClusterService(hz).getThisAddress();
    }

    public static Address[] getAddresses(HecatonInstance[] instances) {
        Address[] addresses = new Address[instances.length];
        for (int i = 0; i < addresses.length; i++) {
            addresses[i] = getAddress(instances[i]);
        }
        return addresses;
    }

    public static Address getAddress(HecatonInstance hz, EndpointQualifier qualifier) {
        return new Address(getClusterService(hz).getLocalMember().getSocketAddress(qualifier));
    }

    /**
     * Returns the partition ID from a non-partitioned Hecaton data
     * structures like {@link ISet}.
     * <p>
     * The partition ID is read via reflection from the internal
     * {@code partitionId} field. This is needed to support proxied
     * Hecaton instances from {@link HecatonStarter}.
     *
     * @param hecatonDataStructure the Hecaton data structure instance
     * @return the partition ID of that data structure
     */
    public static int getPartitionIdViaReflection(Object hecatonDataStructure) {
        try {
            return (Integer) getFieldValueReflectively(hecatonDataStructure, "partitionId");
        } catch (IllegalAccessException e) {
            throw new AssertionError("Cannot retrieve partitionId field from class "
                    + hecatonDataStructure.getClass().getName());
        }
    }

    public static HecatonInstance getFirstBackupInstance(HecatonInstance[] instances, int partitionId) {
        return getBackupInstance(instances, partitionId, 1);
    }

    public static HecatonInstance getBackupInstance(HecatonInstance[] instances, int partitionId, int replicaIndex) {
        InternalPartition partition = getPartitionService(instances[0]).getPartition(partitionId);
        Address backupAddress = partition.getReplicaAddress(replicaIndex);
        for (HecatonInstance instance : instances) {
            if (instance.getCluster().getLocalMember().getAddress().equals(backupAddress)) {
                return instance;
            }
        }
        throw new AssertionError("Could not find backup member for partition " + partitionId);
    }

    /**
     * Obtains a list of {@link IndexRegistry} for the given map local to the node
     * associated with it.
     * <p>
     * There may be more than one indexes instance associated with a map if its
     * indexes are partitioned.
     *
     * @param map the map to obtain the indexes for.
     * @return the obtained indexes list.
     */
    public static List<IndexRegistry> getAllIndexes(IMap map) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        String mapName = mapProxy.getName();
        NodeEngine nodeEngine = mapProxy.getNodeEngine();
        IPartitionService partitionService = nodeEngine.getPartitionService();
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);

        IndexRegistry maybeGlobalIndexes = mapContainer.getGlobalIndexRegistry();
        if (maybeGlobalIndexes != null) {
            return Collections.singletonList(maybeGlobalIndexes);
        }

        List<IndexRegistry> allIndexes = new ArrayList<>();

        int partitionCount = partitionService.getPartitionCount();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            IPartition partition = partitionService.getPartition(partitionId);
            if (!partition.isLocal()) {
                continue;
            }

            IndexRegistry partitionIndexes = mapContainer.getOrNullPartitionedIndexRegistry(partitionId);
            if (partitionIndexes == null) {
                continue;
            }
            assert !partitionIndexes.isGlobal();
            allIndexes.add(partitionIndexes);
        }

        return allIndexes;
    }

    public static PartitionReplicaStateChecker getPartitionReplicaStateChecker(HecatonInstance instance) {
        return getPartitionService(instance).getPartitionReplicaStateChecker();
    }
}
