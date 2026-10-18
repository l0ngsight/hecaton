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

package uk.org.hecaton.client.map;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.PartitionLostEventImpl;
import uk.org.hecaton.map.MapPartitionLostEvent;
import uk.org.hecaton.map.TestEventCollectingMapPartitionLostListener;
import uk.org.hecaton.map.impl.MapPartitionLostEventFilter;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.listener.MapPartitionLostListener;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.UUID;

import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapPartitionLostListenerTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void test_mapPartitionLostListener_registered() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance(getConfig());
        HecatonInstance client = hecatonFactory.newHecatonClient(getClientConfig());

        String mapName = randomMapName();

        client.getMap(mapName).addPartitionLostListener(mock(MapPartitionLostListener.class));

        assertRegistrationEventually(instance, mapName, true);
    }

    @Test
    public void test_mapPartitionLostListener_removed() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance(getConfig());
        HecatonInstance client = hecatonFactory.newHecatonClient(getClientConfig());

        String mapName = randomMapName();

        UUID registrationId = client.getMap(mapName).addPartitionLostListener(mock(MapPartitionLostListener.class));
        assertRegistrationEventually(instance, mapName, true);

        assertTrue(client.getMap(mapName).removePartitionLostListener(registrationId));
        assertRegistrationEventually(instance, mapName, false);
    }

    @Test
    public void test_mapPartitionLostListener_invoked() {
        String mapName = randomMapName();
        Config config = getConfig();
        config.getMapConfig(mapName).setBackupCount(0);
        ClientConfig clientConfig = getClientConfig();

        HecatonInstance instance = hecatonFactory.newHecatonInstance(config);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        warmUpPartitions(instance, client);

        TestEventCollectingMapPartitionLostListener listener = new TestEventCollectingMapPartitionLostListener(0);
        client.getMap(mapName).addPartitionLostListener(listener);

        MapService mapService = getNode(instance).getNodeEngine().getService(MapService.SERVICE_NAME);
        int partitionId = 5;
        mapService.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, null));

        assertMapPartitionLostEventEventually(listener, partitionId);
    }

    @Test
    public void test_mapPartitionLostListener_invoked_fromOtherNode() {
        String mapName = randomMapName();
        Config config = getConfig();
        config.getMapConfig(mapName).setBackupCount(0);

        HecatonInstance instance1 = hecatonFactory.newHecatonInstance(config);
        ClientConfig clientConfig = getClientConfig();
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        HecatonInstance instance2 = hecatonFactory.newHecatonInstance(config);

        TestEventCollectingMapPartitionLostListener listener = new TestEventCollectingMapPartitionLostListener(0);
        client.getMap(mapName).addPartitionLostListener(listener);

        assertRegistrationEventually(instance1, mapName, true);
        assertRegistrationEventually(instance2, mapName, true);

        MapService mapService = getNode(instance2).getNodeEngine().getService(SERVICE_NAME);
        int partitionId = 5;
        mapService.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, null));

        assertMapPartitionLostEventEventually(listener, partitionId);
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private static void assertMapPartitionLostEventEventually(final TestEventCollectingMapPartitionLostListener listener,
                                                              final int partitionId) {
        assertTrueEventually(() -> {

            final List<MapPartitionLostEvent> events = listener.getEvents();
            assertFalse(events.isEmpty());
            assertEquals(partitionId, events.get(0).getPartitionId());

        });
    }

    private static void assertRegistrationEventually(final HecatonInstance instance, final String mapName,
                                                     final boolean shouldBeRegistered) {
        final EventService eventService = getNode(instance).getNodeEngine().getEventService();
        assertTrueEventually(() -> {
            boolean registered = false;
            for (EventRegistration registration : eventService.getRegistrations(SERVICE_NAME, mapName)) {
                if (registration.getFilter() instanceof MapPartitionLostEventFilter) {
                    registered = true;
                    break;
                }
            }
            if (shouldBeRegistered != registered) {
                fail("shouldBeRegistered: " + shouldBeRegistered + " registered: " + registered);
            }
        });
    }
}
