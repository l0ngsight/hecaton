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

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.OperationTimeoutException;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.impl.operationservice.SelfResponseOperation;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static uk.org.hecaton.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationServiceImpl_timeoutTest extends HecatonTestSupport {

    //there was a memory leak caused by the invocation not releasing the backup registration when there is a timeout.
    @Test
    public void testTimeoutSingleMember() throws InterruptedException {
        HecatonInstance hz = createHecatonInstance();
        final IQueue<Object> q = hz.getQueue("queue");

        for (int k = 0; k < 1000; k++) {
            Object response = q.poll(1, TimeUnit.MILLISECONDS);
            assertNull(response);
        }

        OperationServiceImpl_BasicTest.assertNoLitterInOpService(hz);
    }

    //there was a memory leak caused by the invocation not releasing the backup registration when there is a timeout.
    @Test
    public void testTimeoutWithMultiMemberCluster() throws InterruptedException {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance hz1 = factory.newHecatonInstance();
        HecatonInstance hz2 = factory.newHecatonInstance();
        final IQueue<Object> q = hz1.getQueue("queue");

        for (int k = 0; k < 1000; k++) {
            Object response = q.poll(1, TimeUnit.MILLISECONDS);
            assertNull(response);
        }

        OperationServiceImpl_BasicTest.assertNoLitterInOpService(hz1);
        OperationServiceImpl_BasicTest.assertNoLitterInOpService(hz2);
    }

    @Test
    public void testSyncOperationTimeoutSingleMember() {
        testOperationTimeout(1, false);
    }

    @Test
    public void testSyncOperationTimeoutMultiMember() {
        testOperationTimeout(3, false);
    }

    @Test
    public void testAsyncOperationTimeoutSingleMember() {
        testOperationTimeout(1, true);
    }

    @Test
    public void testAsyncOperationTimeoutMultiMember() {
        testOperationTimeout(3, true);
    }

    private void testOperationTimeout(int memberCount, boolean async) {
        assertTrue(memberCount > 0);
        Config config = new Config();
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "3000");

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(memberCount);
        HecatonInstance[] instances = factory.newInstances(config);
        warmUpPartitions(instances);

        final HecatonInstance hz = instances[memberCount - 1];
        NodeEngine nodeEngine = getNodeEngineImpl(hz);
        OperationService operationService = nodeEngine.getOperationService();
        int partitionId = (int) (Math.random() * nodeEngine.getPartitionService().getPartitionCount());

        InternalCompletableFuture<Object> future = operationService
                .invokeOnPartition(null, new TimedOutBackupAwareOperation(), partitionId);

        final CountDownLatch latch = new CountDownLatch(1);
        if (async) {
            future.exceptionally((throwable) -> {
                if (throwable instanceof OperationTimeoutException) {
                    latch.countDown();
                }
                return null;
            });
        } else {
            try {
                future.joinInternal();
                fail("Should throw OperationTimeoutException!");
            } catch (OperationTimeoutException ignored) {
                latch.countDown();
            }
        }

        assertOpenEventually("Should throw OperationTimeoutException", latch);

        for (HecatonInstance instance : instances) {
            OperationServiceImpl_BasicTest.assertNoLitterInOpService(instance);
        }
    }

    // Implements SelfResponseOperation marker to allow invoking without returning a response
    static class TimedOutBackupAwareOperation extends Operation implements BackupAwareOperation, SelfResponseOperation {
        @Override
        public void run() throws Exception {
            LockSupport.parkNanos((long) (Math.random() * 1000 + 10));
        }

        @Override
        public boolean returnsResponse() {
            // required for operation timeout
            return false;
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return 0;
        }

        @Override
        public int getAsyncBackupCount() {
            return 0;
        }

        @Override
        public Operation getBackupOperation() {
            return null;
        }
    }

    @Test
    public void testOperationTimeoutForLongRunningLocalOperation() throws Exception {
        int callTimeoutMillis = 500;
        Config config = new Config();
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), String.valueOf(callTimeoutMillis));

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        HecatonInstance hz1 = factory.newHecatonInstance(config);

        // invoke on the "local" member
        Address localAddress = getNode(hz1).getThisAddress();
        OperationService operationService = getNode(hz1).getNodeEngine().getOperationService();
        InternalCompletableFuture<Boolean> future = operationService
                .invokeOnTarget(null, new SleepingOperation(callTimeoutMillis * 5), localAddress);

        // wait more than operation timeout
        sleepAtLeastMillis(callTimeoutMillis * 3);
        assertTrue(future.get());
    }

    public static class SleepingOperation extends Operation {
        private long sleepTime;

        public SleepingOperation() {
        }

        public SleepingOperation(long sleepTime) {
            this.sleepTime = sleepTime;
        }

        @Override
        public void run() throws Exception {
            sleepAtLeastMillis(sleepTime);
        }

        @Override
        public Object getResponse() {
            return Boolean.TRUE;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeLong(sleepTime);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            sleepTime = in.readLong();
        }
    }

}
