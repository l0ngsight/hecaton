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

package uk.org.hecaton.map;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.partition.IPartition;
import uk.org.hecaton.internal.partition.IPartitionLostEvent;
import uk.org.hecaton.internal.partition.PartitionLostEventImpl;
import uk.org.hecaton.map.impl.MapPartitionLostEventFilter;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.listener.EntryAddedListener;
import uk.org.hecaton.partition.AbstractPartitionLostListenerTest;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapPartitionLostListenerTest extends AbstractPartitionLostListenerTest {

    @Override
    public int getNodeCount() {
        return 2;
    }

    @Test
    public void test_partitionLostListenerInvoked() {
        List<HecatonInstance> instances = getCreatedInstancesShuffledAfterWarmedUp(1);
        HecatonInstance instance = instances.get(0);

        final TestEventCollectingMapPartitionLostListener listener = new TestEventCollectingMapPartitionLostListener(0);
        instance.getMap(getIthMapName(0)).addPartitionLostListener(listener);

        final IPartitionLostEvent internalEvent = new PartitionLostEventImpl(1, 0, null);

        MapService mapService = getNode(instance).getNodeEngine().getService(MapService.SERVICE_NAME);
        mapService.onPartitionLost(internalEvent);

        assertEventEventually(listener, internalEvent);
    }

    @Test
    public void test_allPartitionLostListenersInvoked() {
        List<HecatonInstance> instances = getCreatedInstancesShuffledAfterWarmedUp(2);
        HecatonInstance instance1 = instances.get(0);
        HecatonInstance instance2 = instances.get(0);

        final TestEventCollectingMapPartitionLostListener listener1 = new TestEventCollectingMapPartitionLostListener(0);
        final TestEventCollectingMapPartitionLostListener listener2 = new TestEventCollectingMapPartitionLostListener(0);
        instance1.getMap(getIthMapName(0)).addPartitionLostListener(listener1);
        instance2.getMap(getIthMapName(0)).addPartitionLostListener(listener2);

        final IPartitionLostEvent internalEvent = new PartitionLostEventImpl(1, 0, null);

        MapService mapService = getNode(instance1).getNodeEngine().getService(MapService.SERVICE_NAME);
        mapService.onPartitionLost(internalEvent);

        assertEventEventually(listener1, internalEvent);
        assertEventEventually(listener2, internalEvent);
    }

    @Test
    public void test_partitionLostListenerInvoked_whenEntryListenerIsAlsoRegistered() {
        List<HecatonInstance> instances = getCreatedInstancesShuffledAfterWarmedUp(1);
        HecatonInstance instance = instances.get(0);

        final TestEventCollectingMapPartitionLostListener listener = new TestEventCollectingMapPartitionLostListener(0);
        instance.getMap(getIthMapName(0)).addPartitionLostListener(listener);
        instance.getMap(getIthMapName(0)).addEntryListener(mock(EntryAddedListener.class), true);

        final IPartitionLostEvent internalEvent = new PartitionLostEventImpl(1, 0, null);

        MapService mapService = getNode(instance).getNodeEngine().getService(MapService.SERVICE_NAME);
        mapService.onPartitionLost(internalEvent);

        assertEventEventually(listener, internalEvent);
    }

    @Test
    public void test_partitionLostListenerInvoked_whenNodeCrashed() {
        List<HecatonInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        HecatonInstance survivingInstance = instances.get(0);
        HecatonInstance terminatingInstance = instances.get(1);

        final TestEventCollectingMapPartitionLostListener listener = new TestEventCollectingMapPartitionLostListener(0);
        survivingInstance.getMap(getIthMapName(0)).addPartitionLostListener(listener);

        final Set<Integer> survivingPartitionIds = new HashSet<>();
        Node survivingNode = getNode(survivingInstance);
        Address survivingAddress = survivingNode.getThisAddress();

        for (IPartition partition : survivingNode.getPartitionService().getPartitions()) {
            if (survivingAddress.equals(partition.getReplicaAddress(0))) {
                survivingPartitionIds.add(partition.getPartitionId());
            }
        }

        terminatingInstance.getLifecycleService().terminate();
        waitAllForSafeState(survivingInstance);

        assertTrueEventually(() -> {
            List<MapPartitionLostEvent> events = listener.getEvents();
            assertFalse(events.isEmpty());

            for (MapPartitionLostEvent event : events) {
                assertFalse(survivingPartitionIds.contains(event.getPartitionId()));
            }
        });
    }

    @Test
    public void testMapPartitionLostEventFilter() {
        MapPartitionLostEventFilter filter = new MapPartitionLostEventFilter();
        assertEquals(new MapPartitionLostEventFilter(), filter);
        assertFalse(filter.eval(null));
    }

    private static void assertEventEventually(final TestEventCollectingMapPartitionLostListener listener,
                                              final IPartitionLostEvent internalEvent) {
        assertTrueEventually(() -> {
            List<MapPartitionLostEvent> events = listener.getEvents();
            assertEquals(1, events.size());
            MapPartitionLostEvent event = events.get(0);
            assertEquals(internalEvent.getPartitionId(), event.getPartitionId());
            assertNotNull(event.toString());
        });
    }
}
