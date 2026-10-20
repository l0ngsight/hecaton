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
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.MembershipAdapter;
import uk.org.hecaton.cluster.MembershipEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientReconnectTest extends ClientTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testClientReconnectOnClusterDown() {
        final HecatonInstance h1 = hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        final HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        ReconnectListener reconnectListener = new ReconnectListener();
        client.getLifecycleService().addLifecycleListener(reconnectListener);
        IMap<String, String> m = client.getMap("default");
        h1.shutdown();
        hecatonFactory.newHecatonInstance();
        assertOpenEventually(reconnectListener.reconnectedLatch);
        assertNull(m.put("test", "test"));
        assertEquals("test", m.get("test"));
    }

    @Test
    public void testReconnectToNewInstanceAtSameAddress() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        Address localAddress = instance.getCluster().getLocalMember().getAddress();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        final HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        final CountDownLatch memberRemovedLatch = new CountDownLatch(1);
        client.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemovedLatch.countDown();
            }
        });

        instance.shutdown();
        final HecatonInstance instance2 = hecatonFactory.newHecatonInstance(localAddress);

        assertOpenEventually(memberRemovedLatch);

        assertTrueEventually(() -> {
            assertClusterSize(1, client);
            Iterator<Member> iterator = client.getCluster().getMembers().iterator();
            Member member = iterator.next();
            assertEquals(instance2.getCluster().getLocalMember(), member);
        });
    }

    @Test
    public void testClientShutdownIfReconnectionNotPossible() {
        HecatonInstance server = hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == LifecycleEvent.LifecycleState.SHUTDOWN) {
                shutdownLatch.countDown();
            }
        });
        server.shutdown();

        assertOpenEventually(shutdownLatch);
    }

    @Test(expected = HecatonClientNotActiveException.class)
    public void testRequestShouldFailOnShutdown() {
        final HecatonInstance server = hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        IMap<Object, Object> test = client.getMap("test");
        test.put("key", "value");
        server.shutdown();
        test.get("key");
    }

    @Test
    public void testCallbackAfterServerShutdown() {
        final HecatonInstance server = hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();

        IMap<Object, Object> test = client.getMap("test");
        UUID serverUuid = server.getLocalEndpoint().getUuid();
        server.shutdown();

        // wait until client disconnects from the shutdown member to not get
        // target disconnected exception in case the putAsync call is made
        // before the connection on the client side is closed.
        makeSureDisconnectedFromServer(client, serverUuid);
        CompletionStage<Object> future = test.putAsync("key", "value");
        client.shutdown();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> reference = new AtomicReference<>();
        future.exceptionally(t -> {
            reference.set(t);
            latch.countDown();
            return null;
        });
        assertOpenEventually(latch);
        assertInstanceOf(HecatonClientNotActiveException.class, reference.get());
    }

    @Test(expected = HecatonClientNotActiveException.class)
    public void testExceptionAfterClientShutdown() {
        hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        IMap<Object, Object> test = client.getMap("test");
        test.put("key", "value");
        client.shutdown();
        //to force weak references to be cleaned and get not active exception from serialization service
        System.gc();
        test.get("key");
    }

    @Test(expected = HecatonClientNotActiveException.class)
    public void testExceptionAfterClientShutdown_fromClientConnectionManager() {
        hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        IMap<Object, Object> test = client.getMap("test");
        test.put("key", "value");
        client.shutdown();
        test.size();
    }

    @Test
    public void testShutdownClient_whenThereIsNoCluster() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        client.shutdown();
    }
}
