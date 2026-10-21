/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.client.cp;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPGroupAvailabilityListenerTest.CountingDownCPGroupAvailabilityListener;
import uk.org.hecaton.cp.CPGroupAvailabilityListenerTest.GracefulShutdownAvailabilityListener;
import uk.org.hecaton.cp.internal.HecatonRaftTestSupport;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCPGroupAvailabilityListenerTest extends HecatonRaftTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @Override
    protected TestHecatonInstanceFactory createTestFactory() {
        return factory;
    }

    @Test
    public void whenRegisteredInConfig_thenReceiveEvents() {
        HecatonInstance[] instances = newInstances(3);

        CountingDownCPGroupAvailabilityListener listener = new CountingDownCPGroupAvailabilityListener(1, 1);
        ClientConfig config = new ClientConfig();
        config.addListenerConfig(new ListenerConfig().setImplementation(listener));
        factory.newHecatonClient(config);

        instances[2].getLifecycleService().terminate();
        assertOpenEventually(listener.availabilityLatch);
    }

    @Test
    public void whenGroupMemberTerminated_thenReceiveGroupAvailabilityEvents() throws Exception {
        CountingDownCPGroupAvailabilityListener listener = new CountingDownCPGroupAvailabilityListener(1, 1);

        HecatonInstance[] instances = newInstances(3);
        HecatonInstance client = factory.newHecatonClient();
        client.getCPSubsystem().addGroupAvailabilityListener(listener);

        instances[0].getLifecycleService().terminate();
        assertOpenEventually(listener.availabilityLatch);
        assertFalse(listener.majorityLatch.await(1, TimeUnit.SECONDS));

        instances[2].getLifecycleService().terminate();
        assertOpenEventually(listener.majorityLatch);
    }

    @Test
    public void whenMemberTerminated_thenReceiveCpSubsystemAvailabilityEvents() throws Exception {
        CountingDownCPGroupAvailabilityListener listener = new CountingDownCPGroupAvailabilityListener(1, 1);

        HecatonInstance[] instances = newInstances(5);
        HecatonInstance client = factory.newHecatonClient();
        client.getCPSubsystem().addGroupAvailabilityListener(listener);

        instances[0].getLifecycleService().terminate();
        assertOpenEventually(listener.availabilityLatch);
        assertEquals(1, listener.availabilityEventCount.get());

        instances[4].getLifecycleService().terminate();
        assertFalse(listener.majorityLatch.await(1, TimeUnit.SECONDS));
        assertEquals(2, listener.availabilityEventCount.get());

        instances[2].getLifecycleService().terminate();
        assertOpenEventually(listener.majorityLatch);
        assertEquals(1, listener.majorityEventCount.get());
    }

    @Test
    public void whenListenerDeregistered_thenNoEventsReceived() {
        CountingDownCPGroupAvailabilityListener listener = new CountingDownCPGroupAvailabilityListener(1, 1);

        HecatonInstance[] instances = newInstances(3);
        HecatonInstance client = factory.newHecatonClient();
        UUID id = client.getCPSubsystem().addGroupAvailabilityListener(listener);

        instances[0].getLifecycleService().terminate();
        assertEqualsEventually(1, listener.availabilityEventCount);

        assertTrue(client.getCPSubsystem().removeGroupAvailabilityListener(id));

        instances[2].getLifecycleService().terminate();
        assertTrueAllTheTime(() -> assertEquals(0, listener.majorityEventCount.get()), 3);
    }

    @Test
    public void whenMultipleListenersRegistered_thenAllReceiveEvents() {
        HecatonInstance[] instances = newInstances(3);
        HecatonInstance client = factory.newHecatonClient();

        CountingDownCPGroupAvailabilityListener listener1 = new CountingDownCPGroupAvailabilityListener(1, 1);
        client.getCPSubsystem().addGroupAvailabilityListener(listener1);
        CountingDownCPGroupAvailabilityListener listener2 = new CountingDownCPGroupAvailabilityListener(1, 1);
        client.getCPSubsystem().addGroupAvailabilityListener(listener2);

        instances[0].getLifecycleService().terminate();
        assertOpenEventually(listener1.availabilityLatch);
        assertOpenEventually(listener2.availabilityLatch);
        assertEquals(1, listener1.availabilityEventCount.get());
        assertEquals(1, listener2.availabilityEventCount.get());
    }

    @Test
    public void whenShutdown_receiveAvailabilityEvents() {
        HecatonInstance[] instances = newInstances(5);
        HecatonInstance client = factory.newHecatonClient();

        GracefulShutdownAvailabilityListener listener = new GracefulShutdownAvailabilityListener();
        client.getCPSubsystem().addGroupAvailabilityListener(listener);

        long secondsToAssertInvariant = 3;
        instances[0].getLifecycleService().shutdown();
        assertTrueAllTheTime(() -> assertEquals(1, listener.availabilityDecreased.get()), secondsToAssertInvariant);

        instances[1].getLifecycleService().shutdown();
        assertTrueAllTheTime(() -> assertEquals(2, listener.availabilityDecreased.get()), secondsToAssertInvariant);

        instances[2].getLifecycleService().shutdown();
        assertTrueAllTheTime(() -> assertEquals(3, listener.availabilityDecreased.get()), secondsToAssertInvariant);
    }
}
