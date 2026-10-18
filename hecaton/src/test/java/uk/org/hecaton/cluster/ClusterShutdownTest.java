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

package uk.org.hecaton.cluster;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonOverloadException;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeState;
import uk.org.hecaton.spi.impl.operationservice.AbstractWaitNotifyKey;
import uk.org.hecaton.spi.impl.operationservice.BlockingOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.WaitNotifyKey;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterShutdownTest extends HecatonTestSupport {

    @Test
    public void cluster_mustBeShutDown_by_singleMember_when_clusterState_ACTIVE() {
        testClusterShutdownWithSingleMember(ClusterState.ACTIVE);
    }

    @Test
    public void cluster_mustBeShutDown_by_singleMember_when_clusterState_FROZEN() {
        testClusterShutdownWithSingleMember(ClusterState.FROZEN);
    }

    @Test
    public void cluster_mustBeShutDown_by_singleMember_when_clusterState_PASSIVE() {
        testClusterShutdownWithSingleMember(ClusterState.PASSIVE);
    }

    @Test
    public void cluster_mustBeShutDown_by_multipleMembers_when_clusterState_PASSIVE() {
        testClusterShutdownWithMultipleMembers(6, 3);
    }

    @Test
    public void cluster_mustBeShutDown_by_allMembers_when_clusterState_PASSIVE() {
        testClusterShutdownWithMultipleMembers(6, 6);
    }

    @Test
    public void whenClusterIsAlreadyShutdown_thenLifecycleServiceShutdownShouldDoNothing() {
        HecatonInstance instance = testClusterShutdownWithSingleMember(ClusterState.ACTIVE);

        instance.getLifecycleService().shutdown();
    }

    @Test
    public void clusterShutdown_shouldNotBeRejected_byBackpressure() throws Exception {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_COUNT.toString(), "1");
        config.setProperty(ClusterProperty.BACKPRESSURE_ENABLED.toString(), "true");
        config.setProperty(ClusterProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS.toString(), "100");
        config.setProperty(ClusterProperty.BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION.toString(), "3");

        HecatonInstance hz = createHecatonInstance(config);
        final OperationServiceImpl operationService = getOperationService(hz);
        final Address address = getAddress(hz);

        for (int i = 0; i < 10; i++) {
            Future<Object> future = spawn(() -> {
                operationService.invokeOnTarget(null, new AlwaysBlockingOperation(), address);
                return null;
            });
            try {
                future.get();
            } catch (ExecutionException e) {
                assertInstanceOf(HecatonOverloadException.class, e.getCause());
            }
        }

        Node node = getNode(hz);
        hz.getCluster().shutdown();

        assertFalse(hz.getLifecycleService().isRunning());
        assertEquals(NodeState.SHUT_DOWN, node.getState());
    }

    private HecatonInstance testClusterShutdownWithSingleMember(ClusterState clusterState) {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(4);
        HecatonInstance[] instances = factory.newInstances();
        assertClusterSizeEventually(4, instances);

        HecatonInstance hz1 = instances[0];
        Node[] nodes = getNodes(instances);

        hz1.getCluster().changeClusterState(clusterState);
        hz1.getCluster().shutdown();

        assertNodesShutDownEventually(nodes);

        return hz1;
    }

    private void testClusterShutdownWithMultipleMembers(int clusterSize, int nodeCountToTriggerShutdown) {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(clusterSize);
        HecatonInstance[] instances = factory.newInstances();
        assertClusterSizeEventually(clusterSize, instances);

        instances[0].getCluster().changeClusterState(ClusterState.PASSIVE);
        Node[] nodes = getNodes(instances);

        final CountDownLatch latch = new CountDownLatch(1);

        for (int i = 0; i < nodeCountToTriggerShutdown; i++) {
            final HecatonInstance instance = instances[i];
            final Runnable shutdownRunnable = () -> {
                assertOpenEventually(latch);
                instance.getCluster().shutdown();
            };

            new Thread(shutdownRunnable).start();
        }

        latch.countDown();
        assertNodesShutDownEventually(nodes);
    }

    public static Node[] getNodes(HecatonInstance[] instances) {
        Node[] nodes = new Node[instances.length];
        for (int i = 0; i < instances.length; i++) {
            nodes[i] = getNode(instances[i]);
        }
        return nodes;
    }

    public static void assertNodesShutDownEventually(Node[] nodes) {
        for (final Node node : nodes) {
            assertTrueEventually(() -> assertEquals(NodeState.SHUT_DOWN, node.getState()));
        }
    }

    private static class AlwaysBlockingOperation extends Operation implements BlockingOperation {

        @Override
        public void run() {
        }

        @Override
        public WaitNotifyKey getWaitKey() {
            return new AbstractWaitNotifyKey(getServiceName(), "test") {
            };
        }

        @Override
        public boolean shouldWait() {
            return true;
        }

        @Override
        public void onWaitExpire() {
            sendResponse(new TimeoutException());
        }

        @Override
        public String getServiceName() {
            return "AlwaysBlockingOperationService";
        }
    }
}
