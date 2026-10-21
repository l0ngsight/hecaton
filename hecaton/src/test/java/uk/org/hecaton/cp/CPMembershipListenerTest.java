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

package uk.org.hecaton.cp;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.event.CPMembershipEvent;
import uk.org.hecaton.cp.event.CPMembershipEvent.EventType;
import uk.org.hecaton.cp.event.CPMembershipListener;
import uk.org.hecaton.cp.internal.HecatonRaftTestSupport;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPMembershipListenerTest extends HecatonRaftTestSupport {

    @Test
    public void whenClusterStarts_thenReceiveMemberAddedEvents() {
        int cpCount = 3;
        Config config0 = createConfig(cpCount, cpCount);
        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(cpCount, 0);
        config0.addListenerConfig(new ListenerConfig().setImplementation(listener));

        Config config = createConfig(cpCount, cpCount);

        HecatonInstance[] instances = new HecatonInstance[cpCount];
        instances[0] = factory.newHecatonInstance(config0);

        for (int i = 1; i < cpCount; i++) {
            instances[i] = factory.newHecatonInstance(config);
        }
        waitUntilCPDiscoveryCompleted(instances);

        assertOpenEventually(listener.addedLatch);

        Collection<CPMember> members = instances[0].getCPSubsystem()
                .getCPSubsystemManagementService().getCPMembers()
                .toCompletableFuture().join();

        assertEquals(members, listener.addedMembers);
    }

    @Test
    public void whenMemberShutdown_thenReceiveMemberRemovedEvent() {
        HecatonInstance[] instances = newInstances(3);

        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(0, 1);
        instances[0].getCPSubsystem().addMembershipListener(listener);

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

        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(0, 1);
        instances[0].getCPSubsystem().addMembershipListener(listener);

        HecatonInstance instance = instances[instances.length - 1];
        CPMember member = instance.getCPSubsystem().getLocalCPMember();
        instance.getLifecycleService().terminate();

        assertOpenEventually(listener.removedLatch);
        assertEquals(member, listener.removedMembers.get(0));
    }

    @Test
    public void whenMemberPromoted_thenReceiveMemberAddedEvent() {
        HecatonInstance[] instances = newInstances(3, 3, 1);

        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(1, 0);
        instances[0].getCPSubsystem().addMembershipListener(listener);

        HecatonInstance instance = instances[instances.length - 1];
        instance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember().toCompletableFuture().join();

        assertOpenEventually(listener.addedLatch);
        assertEquals(instance.getCPSubsystem().getLocalCPMember(), listener.addedMembers.get(0));
    }

    @Test
    public void whenListenerDeregistered_thenNoEventsReceived() {
        CountingDownCPMembershipListener listener = new CountingDownCPMembershipListener(1, 1);

        HecatonInstance[] instances = newInstances(5);
        UUID id = instances[1].getCPSubsystem().addMembershipListener(listener);

        instances[0].shutdown();
        assertTrueEventually(() -> assertEquals(1, listener.removedMembers.size()));

        assertTrue(instances[1].getCPSubsystem().removeMembershipListener(id));

        instances[2].shutdown();
        assertTrueAllTheTime(() -> assertEquals(1, listener.removedMembers.size()), 3);
    }

    @Test
    public void whenMultipleListenersRegistered_thenAllReceiveEvents() {
        HecatonInstance[] instances = newInstances(3);

        CountingDownCPMembershipListener listener1 = new CountingDownCPMembershipListener(0, 1);
        CountingDownCPMembershipListener listener2 = new CountingDownCPMembershipListener(0, 1);
        instances[0].getCPSubsystem().addMembershipListener(listener1);
        instances[0].getCPSubsystem().addMembershipListener(listener2);

        HecatonInstance instance = instances[instances.length - 1];
        CPMember member = instance.getCPSubsystem().getLocalCPMember();
        instance.shutdown();

        assertOpenEventually(listener1.removedLatch);
        assertOpenEventually(listener2.removedLatch);
        assertEquals(member, listener1.removedMembers.get(0));
        assertEquals(member, listener2.removedMembers.get(0));
    }

    public static class CountingDownCPMembershipListener implements CPMembershipListener {
        public final CountDownLatch addedLatch;
        public final CountDownLatch removedLatch;
        public final List<CPMember> addedMembers = Collections.synchronizedList(new ArrayList<>());
        public final List<CPMember> removedMembers = Collections.synchronizedList(new ArrayList<>());

        public CountingDownCPMembershipListener(int addedEvents, int removedEvents) {
            addedLatch = new CountDownLatch(addedEvents);
            removedLatch = new CountDownLatch(removedEvents);
        }

        @Override
        public void memberAdded(CPMembershipEvent event) {
            assertEquals(EventType.ADDED, event.getType());
            addedMembers.add(event.getMember());
            addedLatch.countDown();
        }

        @Override
        public void memberRemoved(CPMembershipEvent event) {
            assertEquals(EventType.REMOVED, event.getType());
            removedMembers.add(event.getMember());
            removedLatch.countDown();
        }
    }
}
