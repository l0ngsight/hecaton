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
import uk.org.hecaton.internal.partition.IPartitionLostEvent;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.PartitionLostEventImpl;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.partition.PartitionLostListenerStressTest.EventCollectingPartitionLostListener;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Iterables.getLast;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionLostListenerTest extends HecatonTestSupport {

    private TestHecatonInstanceFactory factory;

    private HecatonInstance[] instances;

    @Before
    public void init() {
        factory = createHecatonInstanceFactory(3);
        instances = new HecatonInstance[2];
        instances[0] = factory.newHecatonInstance();
        instances[1] = factory.newHecatonInstance();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void test_partitionLostListenerInvoked() {
        HecatonInstance instance = instances[0];

        EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        instance.getPartitionService().addPartitionLostListener(listener);

        IPartitionLostEvent internalEvent = new PartitionLostEventImpl(1, 0, null);

        NodeEngineImpl nodeEngine = getNode(instance).getNodeEngine();
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        partitionService.onPartitionLost(internalEvent);

        assertEventEventually(listener, internalEvent);
    }

    @Test
    public void test_allPartitionLostListenersInvoked() {
        HecatonInstance instance1 = instances[0];
        HecatonInstance instance2 = instances[1];

        EventCollectingPartitionLostListener listener1 = new EventCollectingPartitionLostListener();
        EventCollectingPartitionLostListener listener2 = new EventCollectingPartitionLostListener();
        instance1.getPartitionService().addPartitionLostListener(listener1);
        instance2.getPartitionService().addPartitionLostListener(listener2);

        IPartitionLostEvent internalEvent = new PartitionLostEventImpl(1, 0, null);

        NodeEngineImpl nodeEngine = getNode(instance1).getNodeEngine();
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        partitionService.onPartitionLost(internalEvent);

        assertEventEventually(listener1, internalEvent);
        assertEventEventually(listener2, internalEvent);
    }

    @Test
    public void test_partitionLostListenerInvoked_whenNodeCrashed() {
        HecatonInstance survivingInstance = instances[0];
        HecatonInstance terminatingInstance = instances[1];

        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        survivingInstance.getPartitionService().addPartitionLostListener(listener);

        Node survivingNode = getNode(survivingInstance);
        final Address survivingAddress = survivingNode.getThisAddress();

        final Set<Integer> survivingPartitionIds = new HashSet<>();
        for (InternalPartition partition : survivingNode.getPartitionService().getInternalPartitions()) {
            if (survivingAddress.equals(partition.getReplicaAddress(0))) {
                survivingPartitionIds.add(partition.getPartitionId());
            }
        }

        terminatingInstance.getLifecycleService().terminate();
        waitAllForSafeState(survivingInstance);

        assertTrueEventually(() -> {
            List<PartitionLostEvent> events = listener.getEvents();
            assertFalse(events.isEmpty());

            for (PartitionLostEvent event : events) {
                assertEquals(survivingAddress, event.getEventSource());
                assertFalse(survivingPartitionIds.contains(event.getPartitionId()));
                assertEquals(0, event.getLostBackupCount());
                assertFalse(event.allReplicasInPartitionLost());
            }
        });
    }

    @Test
    public void test_partitionLostListenerInvoked_whenAllPartitionReplicasCrashed() {
        HecatonInstance lite = factory.newHecatonInstance(new Config().setLiteMember(true));

        warmUpPartitions(instances);
        warmUpPartitions(lite);
        waitAllForSafeState(instances);
        waitInstanceForSafeState(lite);

        final EventCollectingPartitionLostListener listener = new EventCollectingPartitionLostListener();
        lite.getPartitionService().addPartitionLostListener(listener);

        instances[0].getLifecycleService().terminate();
        instances[1].getLifecycleService().terminate();

        assertTrueEventually(() -> {
            List<PartitionLostEvent> events = listener.getEvents();
            assertFalse(events.isEmpty());
            assertTrue(getLast(events).allReplicasInPartitionLost());
        });
    }

    @Test
    public void test_internalPartitionLostEvent_serialization() throws IOException {
        Address address = new Address();
        PartitionLostEventImpl internalEvent = new PartitionLostEventImpl(1, 2, address);

        ObjectDataOutput output = mock(ObjectDataOutput.class);
        internalEvent.writeData(output);

        verify(output).writeInt(1);
        verify(output).writeInt(2);
    }

    @Test
    public void test_internalPartitionLostEvent_deserialization() throws IOException {
        PartitionLostEventImpl internalEvent = new PartitionLostEventImpl();

        ObjectDataInput input = mock(ObjectDataInput.class);
        when(input.readInt()).thenReturn(1, 2);

        internalEvent.readData(input);

        assertEquals(1, internalEvent.getPartitionId());
        assertEquals(2, internalEvent.getLostReplicaIndex());
    }

    @Test
    public void test_internalPartitionLostEvent_toString() {
        assertNotNull(new PartitionLostEventImpl().toString());
    }

    private void assertEventEventually(final EventCollectingPartitionLostListener listener, final IPartitionLostEvent internalEvent) {
        assertTrueEventually(() -> {
            List<PartitionLostEvent> events = listener.getEvents();
            assertEquals(1, events.size());

            PartitionLostEvent event = events.get(0);
            assertEquals(internalEvent.getPartitionId(), event.getPartitionId());
            assertEquals(internalEvent.getLostReplicaIndex(), event.getLostBackupCount());
        });
    }
}
