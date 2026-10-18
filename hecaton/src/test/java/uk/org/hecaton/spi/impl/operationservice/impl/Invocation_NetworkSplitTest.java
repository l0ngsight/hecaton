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

package uk.org.hecaton.spi.impl.operationservice.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.spi.impl.operationparker.impl.OperationParkerImpl;
import uk.org.hecaton.spi.impl.operationservice.AbstractWaitNotifyKey;
import uk.org.hecaton.spi.impl.operationservice.BlockingOperation;
import uk.org.hecaton.spi.impl.operationservice.ExceptionAction;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.WaitNotifyKey;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getClusterService;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static uk.org.hecaton.test.SplitBrainTestSupport.blockCommunicationBetween;
import static uk.org.hecaton.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_NetworkSplitTest extends HecatonTestSupport {

    @Test
    public void testWaitingInvocations_whenNodeSplitFromCluster() {
        SplitAction action = new FullSplitAction();
        testWaitingInvocations_whenNodeSplitFromCluster(action);
    }

    @Test
    public void testWaitingInvocations_whenNodePartiallySplitFromCluster() {
        SplitAction action = new PartialSplitAction();
        testWaitingInvocations_whenNodeSplitFromCluster(action);
    }

    private void testWaitingInvocations_whenNodeSplitFromCluster(SplitAction action) {
        Config config = createConfig();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        final HecatonInstance hz1 = factory.newHecatonInstance(config);
        final HecatonInstance hz2 = factory.newHecatonInstance(config);
        final HecatonInstance hz3 = factory.newHecatonInstance(config);

        warmUpPartitions(hz1, hz2, hz3);
        int partitionId = getPartitionId(hz2);

        Operation op = new AlwaysBlockingOperation();
        Future<Object> future = getNodeEngineImpl(hz3).getOperationService().invokeOnPartition("", op, partitionId);

        assertTrueEventually(() -> {
            final OperationParkerImpl waitNotifyService3 = (OperationParkerImpl) getNodeEngineImpl(hz2).getOperationParker();
            assertEquals(1, waitNotifyService3.getTotalParkedOperationCount());
        });

        // execute the given split action
        action.run(hz1, hz2, hz3);

        unblock(hz1, hz2, hz3);

        // Let node3 detect the split and merge it back to other two.
        ClusterServiceImpl clusterService3 = (ClusterServiceImpl) getClusterService(hz3);
        clusterService3.merge(getAddress(hz1));

        assertClusterSizeEventually(3, hz1, hz2, hz3);

        try {
            future.get(1, TimeUnit.MINUTES);
            fail("Future.get() should fail with a MemberLeftException!");
        } catch (MemberLeftException expected) {
            ignore(expected);
        } catch (Exception e) {
            fail(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    @Test
    public void testWaitNotifyService_whenNodeSplitFromCluster() {
        SplitAction action = new FullSplitAction();
        testWaitNotifyService_whenNodeSplitFromCluster(action);
    }

    @Test
    public void testWaitNotifyService_whenNodePartiallySplitFromCluster() {
        SplitAction action = new PartialSplitAction();
        testWaitNotifyService_whenNodeSplitFromCluster(action);
    }

    private void testWaitNotifyService_whenNodeSplitFromCluster(SplitAction action) {
        Config config = createConfig();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(4);
        final HecatonInstance hz1 = factory.newHecatonInstance(config);
        final HecatonInstance hz2 = factory.newHecatonInstance(config);
        final HecatonInstance hz3 = factory.newHecatonInstance(config);

        warmUpPartitions(hz1, hz2, hz3);
        int partitionId = getPartitionId(hz3);

        getNodeEngineImpl(hz1).getOperationService().invokeOnPartition("", new AlwaysBlockingOperation(), partitionId);

        final OperationParkerImpl waitNotifyService3 = (OperationParkerImpl) getNodeEngineImpl(hz3).getOperationParker();
        assertTrueEventually(() -> assertEquals(1, waitNotifyService3.getTotalParkedOperationCount()));

        action.run(hz1, hz2, hz3);

        // create a new node to prevent same partition assignments
        // after node3 rejoins
        factory.newHecatonInstance(config);

        assertTrueEventually(() -> assertEquals(0, getPartitionService(hz1).getMigrationQueueSize()));

        unblock(hz1, hz2, hz3);

        // Let node3 detect the split and merge it back to other two.
        ClusterServiceImpl clusterService3 = (ClusterServiceImpl) getClusterService(hz3);
        clusterService3.merge(getAddress(hz1));

        assertClusterSizeEventually(4, hz1, hz2, hz3);

        assertEquals(0, waitNotifyService3.getTotalParkedOperationCount());
    }

    private Config createConfig() {
        Config config = new Config();
        config.setClusterName(generateRandomString(10));
        config.setProperty(ClusterProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "10");
        return config;
    }

    private void unblock(HecatonInstance instance1, HecatonInstance instance2, HecatonInstance instance3) {
        unblockCommunicationBetween(instance1, instance2);
        unblockCommunicationBetween(instance1, instance3);
        unblockCommunicationBetween(instance2, instance3);
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

        @Override
        public ExceptionAction onInvocationException(Throwable throwable) {
            return ExceptionAction.THROW_EXCEPTION;
        }
    }

    private interface SplitAction {
        void run(HecatonInstance instance1, HecatonInstance instance2, HecatonInstance instance3);
    }

    private static class FullSplitAction implements SplitAction {

        @Override
        public void run(final HecatonInstance instance1, final HecatonInstance instance2, final HecatonInstance instance3) {
            // Artificially create a network-split
            blockCommunicationBetween(instance1, instance3);
            blockCommunicationBetween(instance2, instance3);

            closeConnectionBetween(instance1, instance3);
            closeConnectionBetween(instance2, instance3);

            assertTrueEventually(() -> {
                assertEquals(2, getNodeEngineImpl(instance1).getClusterService().getSize());
                assertEquals(2, getNodeEngineImpl(instance2).getClusterService().getSize());
                assertEquals(1, getNodeEngineImpl(instance3).getClusterService().getSize());
            });
        }
    }

    private static class PartialSplitAction implements SplitAction {

        @Override
        public void run(final HecatonInstance instance1, final HecatonInstance instance2, final HecatonInstance instance3) {
            // Artificially create a partial network-split;
            // node1 and node2 will be split from node3
            // but node 3 will not be able to detect that.
            blockCommunicationBetween(instance1, instance3);
            blockCommunicationBetween(instance2, instance3);
            suspectMember(getNode(instance1), getNode(instance3));

            assertTrueEventually(() -> {
                assertEquals(2, getNodeEngineImpl(instance1).getClusterService().getSize());
                assertEquals(2, getNodeEngineImpl(instance2).getClusterService().getSize());
                assertEquals(3, getNodeEngineImpl(instance3).getClusterService().getSize());
            });
        }
    }
}
