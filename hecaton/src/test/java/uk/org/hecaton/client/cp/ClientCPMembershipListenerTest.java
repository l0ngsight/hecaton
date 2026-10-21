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
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPMember;
import uk.org.hecaton.cp.CPMembershipListenerTest.CountingDownCPMembershipListener;
import uk.org.hecaton.cp.internal.HecatonRaftTestSupport;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCPMembershipListenerTest extends HecatonRaftTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @Override
    protected TestHecatonInstanceFactory createTestFactory() {
        return factory;
    }

    @Test
    public void whenRegisteredInConfig_thenReceiveEvents() {
        HecatonInstance[] instances = newInstances(3);

        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(0, 1);
        ClientConfig config = new ClientConfig();
        config.addListenerConfig(new ListenerConfig().setImplementation(listener));
        factory.newHecatonClient(config);

        instances[2].shutdown();
        assertOpenEventually(listener.removedLatch);
    }

    @Test
    public void whenMemberShutdown_thenReceiveMemberRemovedEvent() {
        HecatonInstance[] instances = newInstances(3);

        HecatonInstance client = factory.newHecatonClient();
        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(0, 1);
        client.getCPSubsystem().addMembershipListener(listener);

        HecatonInstance instance = instances[instances.length - 1];
        CPMember member = instance.getCPSubsystem().getLocalCPMember();
        instance.shutdown();

        assertOpenEventually(listener.removedLatch);
        assertEquals(member, listener.removedMembers.get(0));
    }

    @Test
    public void whenMemberTerminated_thenReceiveMemberRemovedEvent() {
        Config config = createConfig(3, 3);
        config.getCPSubsystemConfig().setSessionTimeToLiveSeconds(2).setSessionHeartbeatIntervalSeconds(1)
                .setMissingCPMemberAutoRemovalSeconds(3);

        HecatonInstance[] instances = factory.newInstances(config, 3);
        waitUntilCPDiscoveryCompleted(instances);

        HecatonInstance client = factory.newHecatonClient();
        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(0, 1);
        client.getCPSubsystem().addMembershipListener(listener);

        HecatonInstance instance = instances[instances.length - 1];
        CPMember member = instance.getCPSubsystem().getLocalCPMember();
        instance.getLifecycleService().terminate();

        assertOpenEventually(listener.removedLatch);
        assertEquals(member, listener.removedMembers.get(0));
    }

    @Test
    public void whenMemberPromoted_thenReceiveMemberAddedEvent() {
        HecatonInstance[] instances = newInstances(3, 3, 1);

        HecatonInstance client = factory.newHecatonClient();
        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(1, 0);
        client.getCPSubsystem().addMembershipListener(listener);

        HecatonInstance instance = instances[instances.length - 1];
        instance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember().toCompletableFuture().join();

        assertOpenEventually(listener.addedLatch);
        assertEquals(instance.getCPSubsystem().getLocalCPMember(), listener.addedMembers.get(0));
    }

    @Test
    public void whenListenerDeregistered_thenNoEventsReceived() {
        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(1, 1);

        HecatonInstance[] instances = newInstances(5);
        HecatonInstance client = factory.newHecatonClient();
        UUID id = client.getCPSubsystem().addMembershipListener(listener);

        instances[0].shutdown();
        assertTrueEventually(() -> assertEquals(1, listener.removedMembers.size()));

        assertTrue(client.getCPSubsystem().removeMembershipListener(id));

        instances[2].shutdown();
        assertTrueAllTheTime(() -> assertEquals(1, listener.removedMembers.size()), 3);
    }

    @Test
    public void whenMultipleListenersRegistered_thenAllReceiveEvents() {
        HecatonInstance[] instances = newInstances(3);
        HecatonInstance client = factory.newHecatonClient();

        CountingDownCPMembershipListener listener1 = new CountingDownCPMembershipListener(0, 1);
        CountingDownCPMembershipListener listener2 = new CountingDownCPMembershipListener(0, 1);
        client.getCPSubsystem().addMembershipListener(listener1);
        client.getCPSubsystem().addMembershipListener(listener2);

        HecatonInstance instance = instances[instances.length - 1];
        CPMember member = instance.getCPSubsystem().getLocalCPMember();
        instance.shutdown();

        assertOpenEventually(listener1.removedLatch);
        assertOpenEventually(listener2.removedLatch);
        assertEquals(member, listener1.removedMembers.get(0));
        assertEquals(member, listener2.removedMembers.get(0));
    }
}
