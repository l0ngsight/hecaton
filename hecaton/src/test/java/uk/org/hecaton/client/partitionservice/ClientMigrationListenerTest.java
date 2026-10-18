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

package uk.org.hecaton.client.partitionservice;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.MigrationStateImpl;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.partition.MigrationListener;
import uk.org.hecaton.partition.MigrationState;
import uk.org.hecaton.partition.PartitionMigrationListenerTest;
import uk.org.hecaton.partition.PartitionService;
import uk.org.hecaton.partition.ReplicaMigrationEvent;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import static uk.org.hecaton.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static uk.org.hecaton.internal.partition.IPartitionService.SERVICE_NAME;
import static uk.org.hecaton.internal.partition.InternalPartitionService.MIGRATION_EVENT_TOPIC;
import static uk.org.hecaton.partition.PartitionMigrationListenerTest.assertMigrationEventsConsistentWithResult;
import static uk.org.hecaton.partition.PartitionMigrationListenerTest.assertMigrationProcessCompleted;
import static uk.org.hecaton.partition.PartitionMigrationListenerTest.assertMigrationProcessEventsConsistent;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static uk.org.hecaton.test.HecatonTestSupport.waitAllForSafeState;
import static uk.org.hecaton.test.HecatonTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMigrationListenerTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testAddMigrationListener_whenRegisteredByConfig() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig().addListenerConfig(new ListenerConfig(mock(MigrationListener.class)));

        hecatonFactory.newHecatonClient(clientConfig);

        assertRegistrationsSizeEventually(instance, 1);
    }

    @Test
    public void testAddMigrationListener_whenRegisteredByPartitionService() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();

        client.getPartitionService().addMigrationListener(mock(MigrationListener.class));

        assertRegistrationsSizeEventually(instance, 1);
    }

    @Test
    public void testAddMigrationListener_whenListenerRegisteredTwice() {
        hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();
        PartitionService partitionService = client.getPartitionService();
        MigrationListener listener = mock(MigrationListener.class);

        UUID id1 = partitionService.addMigrationListener(listener);
        UUID id2 = partitionService.addMigrationListener(listener);

        assertNotEquals(id1, id2);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveMigrationListener_whenNullListener() {
        hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();
        PartitionService partitionService = client.getPartitionService();

        partitionService.removeMigrationListener(null);
    }

    @Test
    public void testRemoveMigrationListener_whenNonExistingRegistrationId() {
        hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();
        PartitionService partitionService = client.getPartitionService();

        boolean result = partitionService.removeMigrationListener(UuidUtil.newUnsecureUUID());

        assertFalse(result);
    }

    @Test
    public void testRemoveMigrationListener_whenExistingRegistrationId() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();
        PartitionService clientPartitionService = client.getPartitionService();
        MigrationListener listener = mock(MigrationListener.class);

        UUID registrationId = clientPartitionService.addMigrationListener(listener);
        assertRegistrationsSizeEventually(instance, 1);

        boolean removed = clientPartitionService.removeMigrationListener(registrationId);
        assertRegistrationsSizeEventually(instance, 0);
        assertTrue(removed);

        HecatonInstance hz2 = hecatonFactory.newHecatonInstance();
        warmUpPartitions(instance, hz2);

        verifyMigrationListenerNeverInvoked(listener);
    }

    @Test
    public void testMigrationListenerInvoked_whenRegisteredByConfig() {
        PartitionMigrationListenerTest.EventCollectingMigrationListener clientListener = eventCollectingMigrationListener();
        Function<MigrationListener, HecatonInstance> clientSupplier = listener -> {
            ClientConfig clientConfig = new ClientConfig().addListenerConfig(new ListenerConfig(listener));
            return hecatonFactory.newHecatonClient(clientConfig);
        };

        testMigrationListenerInvoked(clientListener, clientSupplier, this::assertMigrationProcess);
    }

    @Test
    public void testMigrationListenerInvoked_whenRegisteredByPartitionService() {
        PartitionMigrationListenerTest.EventCollectingMigrationListener clientListener = eventCollectingMigrationListener();
        Function<MigrationListener, HecatonInstance> clientSupplier = listener -> {
            HecatonInstance client = hecatonFactory.newHecatonClient();
            client.getPartitionService().addMigrationListener(listener);
            return client;
        };

        testMigrationListenerInvoked(clientListener, clientSupplier, this::assertMigrationProcess);
    }

    @Test
    public void testAllMigrationListenerMethodsInvokedOnTheSameThread() {
        SingleThreadMigrationListener clientListener = new SingleThreadMigrationListener();
        Function<MigrationListener, HecatonInstance> clientSupplier = listener -> {
            HecatonInstance client = hecatonFactory.newHecatonClient();
            client.getPartitionService().addMigrationListener(listener);
            return client;
        };

        testMigrationListenerInvoked(clientListener, clientSupplier,
                SingleThreadMigrationListener::assertAllMethodsInvokedOnTheSameThread);
    }

    private <T extends MigrationListener> void testMigrationListenerInvoked(
            T clientListener,
            Function<MigrationListener, HecatonInstance> clientFactory,
            Consumer<T> assertFunction) {

        HecatonInstance instance1 = hecatonFactory.newHecatonInstance();
        HecatonInstance client = clientFactory.apply(clientListener);
        warmUpPartitions(instance1, client);

        // Change to NO_MIGRATION to prevent repartitioning
        // before 2nd member started and ready.
        instance1.getCluster().changeClusterState(ClusterState.NO_MIGRATION);

        HecatonInstance instance2 = hecatonFactory.newHecatonInstance();

        changeClusterStateEventually(instance2, ClusterState.ACTIVE);
        waitAllForSafeState(instance2, instance1, client);

        assertRegistrationsSizeEventually(instance1, 1);
        assertFunction.accept(clientListener);
    }

    private void verifyMigrationListenerNeverInvoked(MigrationListener listener) {
        verify(listener, never()).migrationStarted(any(MigrationStateImpl.class));
        verify(listener, never()).migrationFinished(any(MigrationStateImpl.class));
        verify(listener, never()).replicaMigrationCompleted(any(ReplicaMigrationEvent.class));
        verify(listener, never()).replicaMigrationFailed(any(ReplicaMigrationEvent.class));
    }

    private void assertMigrationProcess(PartitionMigrationListenerTest.EventCollectingMigrationListener listener) {
        PartitionMigrationListenerTest.MigrationEventsPack eventsPack = listener.ensureAndGetSingleEventPack();
        assertMigrationProcessCompleted(eventsPack);
        assertMigrationProcessEventsConsistent(eventsPack);
        assertMigrationEventsConsistentWithResult(eventsPack);
    }

    private PartitionMigrationListenerTest.EventCollectingMigrationListener eventCollectingMigrationListener() {
        return new PartitionMigrationListenerTest.EventCollectingMigrationListener(false);
    }

    private void assertRegistrationsSizeEventually(HecatonInstance instance, int size) {
        assertTrueEventually(() -> {
            EventService eventService = getNode(instance).getNodeEngine().getEventService();
            Collection<EventRegistration> registrations = eventService
                    .getRegistrations(SERVICE_NAME, MIGRATION_EVENT_TOPIC);
            assertEquals(size, registrations.size());
        });
    }

    static class SingleThreadMigrationListener implements MigrationListener {

        private String threadName;
        private volatile boolean finished = false;
        private volatile boolean invokedOnSingleThread = true;

        @Override
        public void migrationStarted(MigrationState state) {
            threadName = Thread.currentThread().getName();
        }

        @Override
        public void migrationFinished(MigrationState state) {
            if (isInvokedFromAnotherThread()) {
                invokedOnSingleThread = false;
            }
            finished = true;
        }

        @Override
        public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
            if (isInvokedFromAnotherThread()) {
                invokedOnSingleThread = false;
            }
        }

        @Override
        public void replicaMigrationFailed(ReplicaMigrationEvent event) {
            if (isInvokedFromAnotherThread()) {
                invokedOnSingleThread = false;
            }
        }

        public void assertAllMethodsInvokedOnTheSameThread() {
            assertTrueEventually(() -> {
                assertTrue(finished);
                assertTrue(invokedOnSingleThread);
            });
        }

        private boolean isInvokedFromAnotherThread() {
            return !Thread.currentThread().getName().equals(threadName);
        }
    }
}
