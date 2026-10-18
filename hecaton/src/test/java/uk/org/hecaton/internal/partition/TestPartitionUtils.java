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

package uk.org.hecaton.internal.partition;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.internal.partition.impl.PartitionReplicaManager;
import uk.org.hecaton.internal.partition.impl.PartitionServiceState;
import uk.org.hecaton.internal.partition.impl.ReplicaFragmentSyncInfo;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.spi.impl.PartitionSpecificRunnable;
import uk.org.hecaton.internal.util.scheduler.ScheduledEntry;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.instance.impl.TestUtil.getNode;
import static uk.org.hecaton.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static uk.org.hecaton.test.HecatonTestSupport.assertOpenEventually;
import static uk.org.hecaton.test.starter.HecatonProxyFactory.proxyObjectForStarter;
import static uk.org.hecaton.test.starter.HecatonStarterUtils.rethrowGuardianException;
import static uk.org.hecaton.test.starter.ReflectionUtils.getDelegateFromMock;
import static uk.org.hecaton.test.starter.ReflectionUtils.getFieldValueReflectively;
import static java.lang.reflect.Proxy.isProxyClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@SuppressWarnings("WeakerAccess")
public final class TestPartitionUtils {

    private TestPartitionUtils() {
    }

    public static PartitionServiceState getPartitionServiceState(HecatonInstance instance) {
        try {
            Node node = getNode(instance);
            InternalPartitionService partitionService = node.getPartitionService();
            if (isProxyClass(instance.getClass())) {
                try {
                    // this is very ugly, but because of a direct field access to Node.nodeEngine in the MigrationManager
                    // constructor, we cannot properly mock or proxy the PartitionReplicaStateChecker class
                    Object delegate = getDelegateFromMock(partitionService);
                    Object partitionReplicaStateChecker = getFieldValueReflectively(delegate, "partitionReplicaStateChecker");
                    Method method = partitionReplicaStateChecker.getClass().getMethod("getPartitionServiceState");
                    Object result = method.invoke(partitionReplicaStateChecker);
                    return (PartitionServiceState) proxyObjectForStarter(TestPartitionUtils.class.getClassLoader(), result);
                } catch (Exception e) {
                    throw rethrowGuardianException(e);
                }
            } else {
                return partitionService.getPartitionReplicaStateChecker().getPartitionServiceState();
            }
        } catch (IllegalArgumentException e) {
            return PartitionServiceState.SAFE;
        }
    }

    public static Map<Integer, PartitionReplicaVersionsView> getAllReplicaVersions(List<HecatonInstance> instances) {
        Map<Integer, PartitionReplicaVersionsView> replicaVersions = new HashMap<>();
        for (HecatonInstance instance : instances) {
            collectOwnedReplicaVersions(getNode(instance), replicaVersions);
        }
        return replicaVersions;
    }

    public static Map<Integer, PartitionReplicaVersionsView> getOwnedReplicaVersions(Node node) {
        Map<Integer, PartitionReplicaVersionsView> ownedReplicaVersions = new HashMap<>();
        collectOwnedReplicaVersions(node, ownedReplicaVersions);
        return ownedReplicaVersions;
    }

    private static void collectOwnedReplicaVersions(Node node, Map<Integer, PartitionReplicaVersionsView> replicaVersions) {
        InternalPartitionService partitionService = node.getPartitionService();
        Address nodeAddress = node.getThisAddress();
        for (IPartition partition : partitionService.getPartitions()) {
            if (nodeAddress.equals(partition.getOwnerOrNull())) {
                int partitionId = partition.getPartitionId();
                replicaVersions.put(partitionId, getPartitionReplicaVersionsView(node, partitionId));
            }
        }
    }

    public static long[] getDefaultReplicaVersions(Node node, int partitionId) {
        return getPartitionReplicaVersionsView(node, partitionId).getVersions(NonFragmentedServiceNamespace.INSTANCE);
    }

    public static PartitionReplicaVersionsView getPartitionReplicaVersionsView(Node node, int partitionId) {
        GetReplicaVersionsRunnable runnable = new GetReplicaVersionsRunnable(node, partitionId);
        node.getNodeEngine().getOperationService().execute(runnable);
        return runnable.getReplicaVersions();
    }

    public static List<ReplicaFragmentSyncInfo> getOngoingReplicaSyncRequests(HecatonInstance instance) {
        return getOngoingReplicaSyncRequests(getNode(instance));
    }

    public static List<ReplicaFragmentSyncInfo> getOngoingReplicaSyncRequests(Node node) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
        return partitionService.getOngoingReplicaSyncRequests();
    }

    public static List<ScheduledEntry<ReplicaFragmentSyncInfo, Void>> getScheduledReplicaSyncRequests(HecatonInstance instance) {
        return getScheduledReplicaSyncRequests(getNode(instance));
    }

    public static List<ScheduledEntry<ReplicaFragmentSyncInfo, Void>> getScheduledReplicaSyncRequests(Node node) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
        return partitionService.getScheduledReplicaSyncRequests();
    }

    public static Map<Integer, List<Address>> getAllReplicaAddresses(List<HecatonInstance> instances) {
        if (instances.isEmpty()) {
            return Collections.emptyMap();
        }

        for (HecatonInstance instance : instances) {
            Node node = getNode(instance);
            if (node.isMaster()) {
                return getAllReplicaAddresses(node);
            }
        }

        return Collections.emptyMap();
    }

    public static Map<Integer, List<Address>> getAllReplicaAddresses(HecatonInstance instance) {
        return getAllReplicaAddresses(getNode(instance));
    }

    public static Map<Integer, List<Address>> getAllReplicaAddresses(Node node) {
        Map<Integer, List<Address>> allReplicaAddresses = new HashMap<>();
        InternalPartitionService partitionService = node.getPartitionService();
        for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
            allReplicaAddresses.put(partitionId, getReplicaAddresses(node, partitionId));
        }

        return allReplicaAddresses;
    }

    public static List<Address> getReplicaAddresses(HecatonInstance instance, int partitionId) {
        return getReplicaAddresses(getNode(instance), partitionId);
    }

    public static List<Address> getReplicaAddresses(Node node, int partitionId) {
        List<Address> replicaAddresses = new ArrayList<>();
        InternalPartitionService partitionService = node.getPartitionService();
        InternalPartition partition = partitionService.getPartition(partitionId);
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            replicaAddresses.add(partition.getReplicaAddress(i));
        }
        return replicaAddresses;
    }

    private static class GetReplicaVersionsRunnable implements PartitionSpecificRunnable {

        private final CountDownLatch latch = new CountDownLatch(1);

        private final Node node;
        private final int partitionId;

        private PartitionReplicaVersionsView replicaVersions;

        GetReplicaVersionsRunnable(Node node, int partitionId) {
            this.node = node;
            this.partitionId = partitionId;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            InternalPartitionService partitionService = node.nodeEngine.getPartitionService();
            PartitionReplicaManager replicaManager =
                    (PartitionReplicaManager) partitionService.getPartitionReplicaVersionManager();

            Collection<ServiceNamespace> namespaces = replicaManager.getNamespaces(partitionId);
            Map<ServiceNamespace, long[]> versionMap = new HashMap<>(namespaces.size());
            Set<ServiceNamespace> dirty = new HashSet<>();
            for (ServiceNamespace ns : namespaces) {
                long[] originalVersions = replicaManager.getPartitionReplicaVersions(partitionId, ns);
                long[] versions = Arrays.copyOf(originalVersions, originalVersions.length);
                versionMap.put(ns, versions);

                if (replicaManager.isPartitionReplicaVersionDirty(partitionId, ns)) {
                    dirty.add(ns);
                }
            }

            if (!versionMap.containsKey(NonFragmentedServiceNamespace.INSTANCE)) {
                versionMap.put(NonFragmentedServiceNamespace.INSTANCE, new long[InternalPartition.MAX_BACKUP_COUNT]);
            }

            replicaVersions = new PartitionReplicaVersionsView(versionMap, dirty);
            latch.countDown();
        }

        PartitionReplicaVersionsView getReplicaVersions() {
            assertOpenEventually(latch, TimeUnit.MINUTES.toSeconds(1));
            return replicaVersions;
        }
    }

    public static void assertAllPartitionsBelongTo(HecatonInstance instance) {
        InternalPartitionService internalPartitionService = getPartitionService(instance);
        int partitionCount = internalPartitionService.getPartitionCount();
        assertEquals(internalPartitionService.getMemberPartitionsIfAssigned(getAddress(instance)).size(), partitionCount);
    }

    public static void assertSomePartitionsBelongTo(HecatonInstance instance, Address address) {
        InternalPartitionService internalPartitionService = getPartitionService(instance);
        assertFalse(internalPartitionService.getMemberPartitionsIfAssigned(address).isEmpty());
    }

    public static void assertSomePartitionsBelongTo(HecatonInstance instance) {
        InternalPartitionService internalPartitionService = getPartitionService(instance);
        assertFalse(internalPartitionService.getMemberPartitionsIfAssigned(getAddress(instance)).isEmpty());
    }

    public static void assertNoPartitionsBelongTo(HecatonInstance instance) {
        InternalPartitionService internalPartitionService = getPartitionService(instance);
        assertEquals(0, internalPartitionService.getMemberPartitionsIfAssigned(getAddress(instance)).size());
    }

    public static String dumpPartitionTable(PartitionTableView partitionTableView) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partitionTableView.length(); i++) {
            sb.append(i).append(" -> [")
              .append(Arrays.toString(partitionTableView.getReplicas(i))).append("]\n");
        }
        return sb.toString();
    }
}
