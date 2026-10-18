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

package uk.org.hecaton.partition;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.partition.IPartition;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.PartitionReplicaVersionsView;
import uk.org.hecaton.internal.partition.impl.ReplicaFragmentSyncInfo;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.internal.util.scheduler.ScheduledEntry;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.internal.partition.TestPartitionUtils.getAllReplicaAddresses;
import static uk.org.hecaton.internal.partition.TestPartitionUtils.getOngoingReplicaSyncRequests;
import static uk.org.hecaton.internal.partition.TestPartitionUtils.getOwnedReplicaVersions;
import static uk.org.hecaton.internal.partition.TestPartitionUtils.getScheduledReplicaSyncRequests;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertNotNull;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractPartitionLostListenerTest extends HecatonTestSupport {

    public enum NodeLeaveType {
        SHUTDOWN,
        TERMINATE
    }

    private TestHecatonInstanceFactory hecatonInstanceFactory;

    @Before
    public final void setup() {
        hecatonInstanceFactory = createHecatonInstanceFactory(getNodeCount());
    }

    @After
    public final void tearDown() {
        hecatonInstanceFactory.terminateAll();
    }

    protected abstract int getNodeCount();

    protected int getMapEntryCount() {
        return 0;
    }

    protected int getMaxParallelReplicaSyncCount() {
        return 20;
    }

    protected final void stopInstances(List<HecatonInstance> instances, NodeLeaveType nodeLeaveType) {
        stopInstances(instances, nodeLeaveType, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    protected final void stopInstances(List<HecatonInstance> instances, final NodeLeaveType nodeLeaveType, int timeoutSeconds) {
        assertNotNull(nodeLeaveType);

        final List<Thread> threads = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(instances.size());
        for (final HecatonInstance instance : instances) {
            threads.add(new Thread(() -> {
                if (nodeLeaveType == NodeLeaveType.SHUTDOWN) {
                    instance.getLifecycleService().shutdown();
                    latch.countDown();
                } else if (nodeLeaveType == NodeLeaveType.TERMINATE) {
                    instance.getLifecycleService().terminate();
                    latch.countDown();
                } else {
                    System.err.println("Invalid node leave type: " + nodeLeaveType);
                }
            }));
        }

        for (Thread t : threads) {
            t.start();
        }

        assertOpenEventually(latch, timeoutSeconds);
    }

    protected final List<HecatonInstance> getCreatedInstancesShuffledAfterWarmedUp() {
        return getCreatedInstancesShuffledAfterWarmedUp(getNodeCount());
    }

    protected final List<HecatonInstance> getCreatedInstancesShuffledAfterWarmedUp(int nodeCount) {
        List<HecatonInstance> instances = createInstances(nodeCount);
        warmUpPartitions(instances);
        Collections.shuffle(instances);
        return instances;
    }

    protected final List<HecatonInstance> createInstances(int nodeCount) {
        List<HecatonInstance> instances = new ArrayList<>();
        Config config = createConfig(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            instances.add(hecatonInstanceFactory.newHecatonInstance(config));
        }
        return instances;
    }

    private Config createConfig(int nodeCount) {
        Config config = getConfig();
        config.setProperty("hecaton.partition.max.parallel.replications", Integer.toString(getMaxParallelReplicaSyncCount()));
        for (int i = 0; i < nodeCount; i++) {
            config.getMapConfig(getIthMapName(i)).setBackupCount(Math.min(i, InternalPartition.MAX_BACKUP_COUNT));
        }
        return config;
    }

    protected final void populateMaps(HecatonInstance instance) {
        for (int i = 0; i < getNodeCount(); i++) {
            Map<Integer, Integer> map = instance.getMap(getIthMapName(i));
            for (int j = 0; j < getMapEntryCount(); j++) {
                map.put(j, j);
            }
        }
    }

    protected final String getIthMapName(int i) {
        return "map-" + i;
    }

    protected final String getIthCacheName(int i) {
        return "cache-" + i;
    }

    protected final Map<Integer, Integer> getMinReplicaIndicesByPartitionId(List<HecatonInstance> instances) {
        Map<Integer, Integer> survivingPartitions = new HashMap<>();
        for (HecatonInstance instance : instances) {
            Node survivingNode = getNode(instance);
            Address survivingNodeAddress = survivingNode.getThisAddress();

            for (IPartition partition : survivingNode.getPartitionService().getPartitions()) {
                if (partition.isOwnerOrBackup(survivingNodeAddress)) {
                    for (int replicaIndex = 0; replicaIndex < getNodeCount(); replicaIndex++) {
                        if (survivingNodeAddress.equals(partition.getReplicaAddress(replicaIndex))) {
                            Integer replicaIndexOfOtherInstance = survivingPartitions.get(partition.getPartitionId());
                            if (replicaIndexOfOtherInstance != null) {
                                survivingPartitions
                                        .put(partition.getPartitionId(), Math.min(replicaIndex, replicaIndexOfOtherInstance));
                            } else {
                                survivingPartitions.put(partition.getPartitionId(), replicaIndex);
                            }
                            break;
                        }
                    }
                }
            }
        }
        return survivingPartitions;
    }

    @SuppressWarnings("SameParameterValue")
    protected final void waitAllForSafeStateAndDumpPartitionServiceOnFailure(List<HecatonInstance> instances,
                                                                             int timeoutInSeconds) {
        try {
            waitAllForSafeState(instances, timeoutInSeconds);
        } catch (AssertionError e) {
            logPartitionState(instances);
            throw e;
        }
    }

    private void logPartitionState(List<HecatonInstance> instances) {
        for (Entry<Integer, List<Address>> entry : getAllReplicaAddresses(instances).entrySet()) {
            System.out.println("PartitionTable >> partitionId=" + entry.getKey() + " table=" + entry.getValue());
        }

        for (HecatonInstance instance : instances) {
            Address address = getNode(instance).getThisAddress();
            for (Entry<Integer, PartitionReplicaVersionsView> entry : getOwnedReplicaVersions(getNode(instance)).entrySet()) {
                PartitionReplicaVersionsView replicaVersionsView = entry.getValue();
                for (ServiceNamespace namespace : replicaVersionsView.getNamespaces()) {
                    System.out.println(namespace + " ReplicaVersions >> " + address + " - partitionId=" + entry.getKey()
                            + " replicaVersions=" + Arrays.toString(replicaVersionsView.getVersions(namespace)));
                }
            }

            for (ReplicaFragmentSyncInfo replicaSyncInfo : getOngoingReplicaSyncRequests(instance)) {
                System.out.println("OngoingReplicaSync >> " + address + " - " + replicaSyncInfo);
            }

            for (ScheduledEntry<ReplicaFragmentSyncInfo, Void> entry : getScheduledReplicaSyncRequests(instance)) {
                System.out.println("ScheduledReplicaSync >> " + address + " - " + entry.getKey());
            }
        }
    }
}
