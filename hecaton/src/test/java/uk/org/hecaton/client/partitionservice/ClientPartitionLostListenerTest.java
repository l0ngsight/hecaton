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
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.PartitionLostEventImpl;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.partition.PartitionLostEvent;
import uk.org.hecaton.partition.PartitionLostListener;
import uk.org.hecaton.partition.PartitionLostListenerStressTest.EventCollectingPartitionLostListener;
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
import java.util.List;
import java.util.UUID;

import static uk.org.hecaton.internal.partition.InternalPartitionService.PARTITION_LOST_EVENT_TOPIC;
import static uk.org.hecaton.internal.partition.InternalPartitionService.SERVICE_NAME;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.HecatonTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientPartitionLostListenerTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void test_partitionLostListener_registeredByConfig() {
        final HecatonInstance instance = hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig().addListenerConfig(new ListenerConfig(mock(PartitionLostListener.class)));
        hecatonFactory.newHecatonClient(clientConfig);

        assertRegistrationsSizeEventually(instance, 4);
    }

    @Test
    public void test_partitionLostListener_registered() {
        final HecatonInstance instance = hecatonFactory.newHecatonInstance();
        final HecatonInstance client = hecatonFactory.newHecatonClient();

        client.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        // Expected = 4 -> 1 added & 1 from {@link uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 4);
    }

    @Test
    public void test_partitionLostListener_removed() {
        final HecatonInstance instance = hecatonFactory.newHecatonInstance();
        final HecatonInstance client = hecatonFactory.newHecatonClient();

        final UUID registrationId = client.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        // Expected = 4 -> 1 added & 1 from {@link uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 4);

        client.getPartitionService().removePartitionLostListener(registrationId);
        // Expected = 3 -> see {@link uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 3);
    }

    @Test
    public void test_partitionLostListener_registeredByConfig_invoked() {
        final HecatonInstance instance = hecatonFactory.newHecatonInstance();
        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        final ClientConfig clientConfig = new ClientConfig().addListenerConfig(new ListenerConfig(listener));
        final HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        warmUpPartitions(instance, client);

        // Expected = 4 -> 1 added & 1 from {@link uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 4);

        final InternalPartitionServiceImpl partitionService = getNode(instance).getNodeEngine().getService(SERVICE_NAME);
        final int partitionId = 5;
        Address address = instance.getCluster().getLocalMember().getAddress();
        partitionService.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, address));

        assertPartitionLostEventEventually(listener, partitionId);
    }

    @Test
    public void test_partitionLostListener_invoked() {
        final HecatonInstance instance = hecatonFactory.newHecatonInstance();
        final HecatonInstance client = hecatonFactory.newHecatonClient();
        warmUpPartitions(instance, client);

        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();

        client.getPartitionService().addPartitionLostListener(listener);
        // Expected = 4 -> 1 added & 1 from {@link uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 4);

        final InternalPartitionServiceImpl partitionService = getNode(instance).getNodeEngine().getService(SERVICE_NAME);
        final int partitionId = 5;
        Address address = instance.getCluster().getLocalMember().getAddress();
        partitionService.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, address));

        assertPartitionLostEventEventually(listener, partitionId);
    }

    @Test
    public void test_partitionLostListener_invoked_fromOtherNode() {
        final HecatonInstance instance1 = hecatonFactory.newHecatonInstance();
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        final HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        final HecatonInstance instance2 = hecatonFactory.newHecatonInstance();
        warmUpPartitions(instance1, instance2, client);

        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        client.getPartitionService().addPartitionLostListener(listener);
        // Expected = 2 -> 1 added & 1 from {@link uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers * instances
        assertRegistrationsSizeEventually(instance1, 7);
        assertRegistrationsSizeEventually(instance2, 7);

        final InternalPartitionServiceImpl partitionService = getNode(instance2).getNodeEngine().getService(SERVICE_NAME);
        final int partitionId = 5;
        Address address = instance1.getCluster().getLocalMember().getAddress();
        partitionService.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, address));

        assertPartitionLostEventEventually(listener, partitionId);
    }

    private void assertRegistrationsSizeEventually(final HecatonInstance instance, final int size) {
        assertTrueEventually(() -> {
            final EventService eventService = getNode(instance).getNodeEngine().getEventService();
            final Collection<EventRegistration> registrations = eventService
                    .getRegistrations(SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC);
            assertEquals(size, registrations.size());
        });
    }

    private void assertPartitionLostEventEventually(final EventCollectingPartitionLostListener listener, final int partitionId) {
        assertTrueEventually(() -> {
            final List<PartitionLostEvent> events = listener.getEvents();
            assertFalse(events.isEmpty());
            assertEquals(partitionId, events.get(0).getPartitionId());

        });
    }

}
