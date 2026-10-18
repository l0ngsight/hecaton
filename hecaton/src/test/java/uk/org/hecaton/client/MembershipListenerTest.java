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

package uk.org.hecaton.client;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.InitialMembershipEvent;
import uk.org.hecaton.cluster.InitialMembershipListener;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.MembershipEvent;
import uk.org.hecaton.cluster.MembershipListener;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.EventObject;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MembershipListenerTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    private static class MemberShipEventLogger implements MembershipListener {

        public LinkedBlockingDeque<EventObject> events = new LinkedBlockingDeque<>();

        @Override
        public void memberAdded(MembershipEvent event) {
            events.addLast(event);
        }

        @Override
        public void memberRemoved(MembershipEvent event) {
            events.addLast(event);
        }

    }

    private static class InitialMemberShipEventLogger implements InitialMembershipListener {

        public LinkedBlockingDeque<EventObject> events = new LinkedBlockingDeque<>();

        @Override
        public void memberAdded(MembershipEvent event) {
            events.addLast(event);
        }

        @Override
        public void memberRemoved(MembershipEvent event) {
            events.addLast(event);
        }

        @Override
        public void init(InitialMembershipEvent event) {
            events.addLast(event);
        }
    }

    @Test
    public void whenMemberAdded_thenMemberAddedEvent() {
        final HecatonInstance server1 = hecatonFactory.newHecatonInstance();
        final MemberShipEventLogger listener = new MemberShipEventLogger();
        HecatonInstance client = hecatonFactory.newHecatonClient();
        client.getCluster().addMembershipListener(listener);

        //start a second server and verify that the listener receives it.
        final HecatonInstance server2 = hecatonFactory.newHecatonInstance();

        //verify that the listener receives member added event.
        assertTrueEventually(() -> {
            assertNotEquals("Expecting one or more events", 0, listener.events.size());
            MembershipEvent event = (MembershipEvent) listener.events.getLast();
            assertEquals("Last event should be member added", MembershipEvent.MEMBER_ADDED, event.getEventType());
            assertEquals(server2.getCluster().getLocalMember(), event.getMember());
            assertEquals(getMembers(server1, server2), event.getMembers());
        });
    }

    @Test
    public void givenMixOfListenerExists_whenConnect_thenCallInitialMembershipListener() {
        hecatonFactory.newHecatonInstance();

        final ClientConfig config = new ClientConfig();

        // first add bunch of *regular* MembershipListener. They do not implement InitialMembershipListener
        config.addListenerConfig(new ListenerConfig().setImplementation(new MemberShipEventLogger()));
        config.addListenerConfig(new ListenerConfig().setImplementation(new MemberShipEventLogger()));
        config.addListenerConfig(new ListenerConfig().setImplementation(new MemberShipEventLogger()));

        // now add an InitialMembershipListener
        // if there is an exception thrown during event delivery to regular listeners
        // then no event will likely be delivered to InitialMemberShipEventLogger
        final InitialMemberShipEventLogger initialListener = new InitialMemberShipEventLogger();
        config.addListenerConfig(new ListenerConfig().setImplementation(initialListener));

        //connect to a grid
        hecatonFactory.newHecatonClient(config);

        assertTrueEventually(() -> {
            assertEquals("Expecting one event", 1, initialListener.events.size());
            InitialMembershipEvent event = (InitialMembershipEvent) initialListener.events.getLast();
            assertEquals(1, event.getMembers().size());
        });
    }

    @Test
    public void whenMemberRemoved_thenMemberRemovedEvent() {
        final HecatonInstance server1 = hecatonFactory.newHecatonInstance();
        final MemberShipEventLogger listener = new MemberShipEventLogger();
        HecatonInstance client = hecatonFactory.newHecatonClient();

        //start a second server and verify that hecatonFactory listener receives it.
        final HecatonInstance server2 = hecatonFactory.newHecatonInstance();

        client.getCluster().addMembershipListener(listener);

        final Member server2Member = server2.getCluster().getLocalMember();
        server2.shutdown();

        //verify that the correct member removed event was received.
        assertTrueEventually(() -> {
            assertNotEquals("Expecting one or more events", 0, listener.events.size());
            MembershipEvent event = (MembershipEvent) listener.events.getLast();
            assertEquals("Last event should be member removed", MembershipEvent.MEMBER_REMOVED, event.getEventType());
            assertEquals(server2Member, event.getMember());
            assertEquals(getMembers(server1), event.getMembers());
        });
    }

    @Test
    public void removedPhantomListener_thenFalse() {
        hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();
        assertFalse(client.getCluster().removeMembershipListener(UuidUtil.newUnsecureUUID()));
    }

    @Test(expected = NullPointerException.class)
    public void removedNullListener_thenException() {
        hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();
        assertFalse(client.getCluster().removeMembershipListener(null));
    }

    @Test(expected = java.lang.NullPointerException.class)
    public void addNullListener_thenException() {
        hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();
        client.getCluster().addMembershipListener(null);
    }

    private Set<Member> getMembers(HecatonInstance... instances) {
        Set<Member> result = new HashSet<>();
        for (HecatonInstance hz : instances) {
            result.add(hz.getCluster().getLocalMember());
        }
        return result;
    }

    /**
     * related to issue #1181
     */
    @Test
    public void testAddInitialMembership_whenListenerAddedViaClientConfig() {
        hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(new ListenerConfig().setImplementation(mock(InitialMembershipListener.class)));
        hecatonFactory.newHecatonClient(clientConfig);
    }

    @Test
    public void initialMemberEvents_whenAddedViaConfig() throws InterruptedException {
        hecatonFactory.newHecatonInstance();
        hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        final InitialMemberShipEventLogger listener = new InitialMemberShipEventLogger();
        clientConfig.addListenerConfig(new ListenerConfig().setImplementation(listener));
        hecatonFactory.newHecatonClient(clientConfig);

        EventObject eventObject = listener.events.poll(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertInstanceOf(InitialMembershipEvent.class, eventObject);
        InitialMembershipEvent event = (InitialMembershipEvent) eventObject;
        assertEquals(2, event.getMembers().size());
        assertEquals(0, listener.events.size());
    }

    @Test
    public void initialMemberEvents_whenAddedAfterClientStarted() throws InterruptedException {
        hecatonFactory.newHecatonInstance();
        hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        final InitialMemberShipEventLogger listener = new InitialMemberShipEventLogger();
        client.getCluster().addMembershipListener(listener);

        EventObject eventObject = listener.events.poll(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertInstanceOf(InitialMembershipEvent.class, eventObject);
        InitialMembershipEvent event = (InitialMembershipEvent) eventObject;
        assertEquals(2, event.getMembers().size());
        assertEquals(0, listener.events.size());
    }

    @Test
    public void initialMemberEvents_whenAddedAfterClientStartedAsync() throws InterruptedException {
        hecatonFactory.newHecatonInstance();
        hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        final InitialMemberShipEventLogger listener = new InitialMemberShipEventLogger();
        client.getCluster().addMembershipListener(listener);

        EventObject eventObject = listener.events.poll(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertInstanceOf(InitialMembershipEvent.class, eventObject);
        InitialMembershipEvent event = (InitialMembershipEvent) eventObject;
        assertEquals(2, event.getMembers().size());
        assertEquals(0, listener.events.size());
    }

    @Test
    public void initialMemberEvents_whenClusterRestarted() throws InterruptedException {
        HecatonInstance instance1 = hecatonFactory.newHecatonInstance();
        HecatonInstance instance2 = hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        final InitialMemberShipEventLogger listener = new InitialMemberShipEventLogger();
        clientConfig.addListenerConfig(new ListenerConfig().setImplementation(listener));
        hecatonFactory.newHecatonClient(clientConfig);

        EventObject eventObject = listener.events.poll(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertInstanceOf(InitialMembershipEvent.class, eventObject);

        instance1.getLifecycleService().terminate();

        eventObject = listener.events.poll(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertInstanceOf(MembershipEvent.class, eventObject);
        assertEquals(MembershipEvent.MEMBER_REMOVED, ((MembershipEvent) eventObject).getEventType());

        instance2.getLifecycleService().terminate();
        hecatonFactory.newHecatonInstance();

        eventObject = listener.events.poll(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertInstanceOf(MembershipEvent.class, eventObject);
        assertEquals(MembershipEvent.MEMBER_REMOVED, ((MembershipEvent) eventObject).getEventType());

        eventObject = listener.events.poll(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertInstanceOf(MembershipEvent.class, eventObject);
        assertEquals(MembershipEvent.MEMBER_ADDED, ((MembershipEvent) eventObject).getEventType());
    }
}
