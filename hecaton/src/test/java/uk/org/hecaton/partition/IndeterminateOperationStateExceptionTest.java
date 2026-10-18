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
import uk.org.hecaton.core.IndeterminateOperationStateException;
import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.server.PacketFilter;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.impl.AllowedDuringPassiveState;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.SpiDataSerializerHook;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;
import uk.org.hecaton.spi.impl.operationservice.SelfResponseOperation;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionalMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static uk.org.hecaton.spi.impl.SpiDataSerializerHook.F_ID;
import static uk.org.hecaton.spi.properties.ClusterProperty.FAIL_ON_INDETERMINATE_OPERATION_STATE;
import static uk.org.hecaton.spi.properties.ClusterProperty.OPERATION_BACKUP_TIMEOUT_MILLIS;
import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.PacketFiltersUtil.dropOperationsBetween;
import static uk.org.hecaton.test.PacketFiltersUtil.dropOperationsFrom;
import static uk.org.hecaton.test.PacketFiltersUtil.wrapCustomerFilter;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndeterminateOperationStateExceptionTest extends HecatonTestSupport {

    private final TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
    private HecatonInstance instance1;
    private HecatonInstance instance2;

    private void setup(boolean enableFailOnIndeterminateOperationState) {
        Config config = getConfig(enableFailOnIndeterminateOperationState);
        instance1 = factory.newHecatonInstance(config);
        instance2 = factory.newHecatonInstance(config);
        warmUpPartitions(instance1, instance2);
    }

    private static Config getConfig(boolean enableFailOnIndeterminateOperationState) {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.setProperty(OPERATION_BACKUP_TIMEOUT_MILLIS.getName(), String.valueOf(3000));
        if (enableFailOnIndeterminateOperationState) {
            config.setProperty(FAIL_ON_INDETERMINATE_OPERATION_STATE.getName(), String.valueOf(true));
        }
        return config;
    }

    @Test
    public void partitionInvocation_shouldFailOnBackupTimeout_whenConfigurationEnabledGlobally() {
        partitionInvocation_shouldFailOnBackupTimeout_whenConfigurationEnabledGlobally(false);
    }

    @Test
    public void partitionInvocation_shouldFailOnBackupTimeout_whenConfigurationEnabledGloballyAndMemberLeft() {
        partitionInvocation_shouldFailOnBackupTimeout_whenConfigurationEnabledGlobally(true);
    }

    private void partitionInvocation_shouldFailOnBackupTimeout_whenConfigurationEnabledGlobally(boolean shutdownMember) {
        setup(true);

        var waitForBackupLatch = waitForBackupAndDrop(instance1);
        int partitionId = getPartitionId(instance1);

        OperationServiceImpl operationService = getNodeEngineImpl(instance1).getOperationService();
        InternalCompletableFuture<Object> future = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new PrimaryOperation(), partitionId).invoke();

        if (shutdownMember) {
            // wait for operation execution on primary and backup to be sent
            assertOpenEventually(waitForBackupLatch);
            instance2.shutdown();
        }

        assertThat(future).failsWithin(2, TimeUnit.MINUTES)
                .withThrowableThat().withCauseInstanceOf(IndeterminateOperationStateException.class);
    }

    @Test
    public void partitionInvocation_shouldFailOnBackupTimeout_whenConfigurationEnabledForInvocation() {
        partitionInvocation_shouldFailOnBackupTimeout_whenConfigurationEnabledForInvocationAndMemberLeft(false);
    }

    @Test
    public void partitionInvocation_shouldFailOnBackupTimeout_whenConfigurationEnabledForInvocationAndMemberLeft() {
        partitionInvocation_shouldFailOnBackupTimeout_whenConfigurationEnabledForInvocationAndMemberLeft(true);
    }

    private void partitionInvocation_shouldFailOnBackupTimeout_whenConfigurationEnabledForInvocationAndMemberLeft(boolean shutdownMember) {
        setup(false);

        var waitForBackupLatch = waitForBackupAndDrop(instance1);
        int partitionId = getPartitionId(instance1);

        OperationServiceImpl operationService = getNodeEngineImpl(instance1).getOperationService();
        InternalCompletableFuture<Object> future = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new PrimaryOperation(), partitionId)
                .setFailOnIndeterminateOperationState(true)
                .invoke();

        if (shutdownMember) {
            // wait for operation execution on primary and backup to be sent
            assertOpenEventually(waitForBackupLatch);
            instance2.shutdown();
        }

        assertThat(future).failsWithin(2, TimeUnit.MINUTES)
                .withThrowableThat().withCauseInstanceOf(IndeterminateOperationStateException.class);
    }

    @Test
    public void partitionInvocation_shouldFailOnBackupTimeout_whenConfigurationEnabledForInvocationAndUnrelatedMemberLeft() throws InterruptedException {
        setup(false);

        var instance3 = factory.newHecatonInstance(getConfig(false));
        assertClusterSizeEventually(3, instance3);
        waitClusterForSafeState(instance3);

        // in this scenario instance2 or instance3 will contain backup partition
        // we drop backups to both to be sure that backup operation is lost
        var waitForBackupLatch = waitForBackupAndDrop(instance1);

        int partitionId = getPartitionId(instance1);

        OperationServiceImpl operationService = getNodeEngineImpl(instance1).getOperationService();
        InternalCompletableFuture<Object> future = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new PrimaryOperation(), partitionId)
                .setFailOnIndeterminateOperationState(true)
                .invoke();

        // wait for operation execution on primary and backup to be sent
        assertOpenEventually(waitForBackupLatch);

        // terminate unrelated member to simulate scenario when member not involved in the operation crashes
        // which should not impact the outcome of the operation (should fail due to lost backup message)
        var unrelatedMemberAddress = Accessors.getPartitionService(instance1).getPartition(partitionId).getReplica(2).address();
        for (var instance : List.of(instance2, instance3)) {
            if (Accessors.getAddress(instance).equals(unrelatedMemberAddress)) {
                instance.getLifecycleService().terminate();
            }
        }

        assertThat(future).failsWithin(2, TimeUnit.MINUTES)
                .withThrowableThat().withCauseInstanceOf(IndeterminateOperationStateException.class);
    }

    @Test
    public void partitionInvocation_shouldFail_whenPartitionPrimaryLeaves() throws InterruptedException, TimeoutException {
        setup(true);

        int partitionId = getPartitionId(instance2);
        OperationServiceImpl operationService = getNodeEngineImpl(instance1).getOperationService();
        InternalCompletableFuture<Object> future = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new SilentOperation(), partitionId).invoke();

        assertTrueEventually(() -> assertTrue(instance2.getUserContext().containsKey(SilentOperation.EXECUTION_STARTED)));

        spawn(() -> instance2.getLifecycleService().terminate());
        assertThat(future).failsWithin(2, TimeUnit.MINUTES)
                .withThrowableThat().withCauseInstanceOf(IndeterminateOperationStateException.class);
    }

    @Test
    public void readOnlyPartitionInvocation_shouldSucceed_whenPartitionPrimaryLeaves()
            throws InterruptedException, TimeoutException, ExecutionException {
        setup(true);

        dropOperationsBetween(instance2, instance1, SpiDataSerializerHook.F_ID, singletonList(SpiDataSerializerHook.NORMAL_RESPONSE));

        int partitionId = getPartitionId(instance2);
        OperationServiceImpl operationService = getNodeEngineImpl(instance1).getOperationService();
        InternalCompletableFuture<Boolean> future = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new DummyReadOperation(), partitionId).invoke();
        spawn(() -> instance2.getLifecycleService().terminate());
        boolean response = future.get(2, TimeUnit.MINUTES);
        assertTrue(response);
        assertEquals(getAddress(instance1), instance1.getUserContext().get(DummyReadOperation.LAST_INVOCATION_ADDRESS));
    }

    @Test
    public void transaction_shouldFail_whenBackupTimeoutOccurs() {
        setup(true);

        dropOperationsBetween(instance1, instance2, F_ID, singletonList(SpiDataSerializerHook.BACKUP));
        dropOperationsBetween(instance2, instance1, F_ID, singletonList(SpiDataSerializerHook.BACKUP));

        String name = randomMapName();
        String key1 = generateKeyOwnedBy(instance1);
        String key2 = generateKeyOwnedBy(instance2);

        TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        try {
            TransactionalMap<Object, Object> map = context.getMap(name);
            map.put(key1, "value");
            map.put(key2, "value");

            context.commitTransaction();
            fail("Should fail with IndeterminateOperationStateException");
        } catch (IndeterminateOperationStateException e) {
            context.rollbackTransaction();
        }

        IMap<Object, Object> map = instance1.getMap(name);
        assertNull(map.get(key1));
        assertNull(map.get(key2));
    }

    @Test(expected = MemberLeftException.class)
    public void targetInvocation_shouldFailWithMemberLeftException_onTargetMemberLeave() throws Exception {
        setup(true);

        OperationServiceImpl operationService = getNodeEngineImpl(instance1).getOperationService();
        Address target = getAddress(instance2);
        InternalCompletableFuture<Object> future = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new SilentOperation(), target).invoke();

        assertTrueEventually(() -> assertTrue(instance2.getUserContext().containsKey(SilentOperation.EXECUTION_STARTED)));

        spawn(() -> instance2.getLifecycleService().terminate());

        future.get(2, TimeUnit.MINUTES);
    }

    /**
     * Drops all backup operations originating from given instance.
     * Allows waiting for first dropped backup.
     *
     * @param from source instance (partition owner)
     * @return latch to wait for first dropped backup
     */
    @Nonnull
    public static CountDownLatch waitForBackupAndDrop(HecatonInstance from) {
        CountDownLatch waitForBackupLatch = new CountDownLatch(1);

        dropOperationsFrom(from, F_ID, singletonList(SpiDataSerializerHook.BACKUP));
        wrapCustomerFilter(from, pf -> (packet, endpoint) -> {
            var action = pf.filter(packet, endpoint);
            if (action == PacketFilter.Action.DROP) {
                waitForBackupLatch.countDown();
            }
            return action;
        });
        return waitForBackupLatch;
    }

    public static class PrimaryOperation extends Operation implements BackupAwareOperation {

        @Override
        public void run() throws Exception {
        }

        @Override
        public boolean returnsResponse() {
            return true;
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return 1;
        }

        @Override
        public int getAsyncBackupCount() {
            return 0;
        }

        @Override
        public Operation getBackupOperation() {
            return new BackupOperation();
        }

    }

    public static class BackupOperation extends Operation {
        public static final String EXECUTION_DONE = "execution-done";

        @Override
        public void run() throws Exception {
            getNodeEngine().getHecatonInstance().getUserContext().put(EXECUTION_DONE, new Object());
        }
    }

    // Implements SelfResponseOperation marker to allow invoking without returning a response
    public static class SilentOperation extends Operation implements AllowedDuringPassiveState, SelfResponseOperation {

        static final String EXECUTION_STARTED = "execution-started";

        @Override
        public void run() throws Exception {
            getNodeEngine().getHecatonInstance().getUserContext().put(EXECUTION_STARTED, new Object());
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }

    }

    public static class DummyReadOperation extends Operation implements ReadonlyOperation {

        static final String LAST_INVOCATION_ADDRESS = "last-invocation-address";

        @Override
        public void run() throws Exception {
            Address address = getNodeEngine().getThisAddress();
            getNodeEngine().getHecatonInstance().getUserContext().put(LAST_INVOCATION_ADDRESS, address);
        }

        @Override
        public boolean returnsResponse() {
            return true;
        }

        @Override
        public Object getResponse() {
            return true;
        }

    }

}
