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

package uk.org.hecaton.client.listeners.leak;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.ClientEndpoint;
import uk.org.hecaton.client.impl.clientside.ClientTestUtil;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.impl.spi.impl.listener.ClientConnectionRegistration;
import uk.org.hecaton.client.impl.spi.impl.listener.ClientListenerServiceImpl;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.collection.ItemListener;
import uk.org.hecaton.core.DistributedObjectListener;
import uk.org.hecaton.core.EntryListener;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.listener.MapListener;
import uk.org.hecaton.map.listener.MapPartitionLostListener;
import uk.org.hecaton.multimap.MultiMap;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.topic.ITopic;
import uk.org.hecaton.topic.MessageListener;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static uk.org.hecaton.test.Accessors.getNode;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ListenerLeakTest extends ClientTestSupport {


    @Parameterized.Parameters(name = "allMembersRouting:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{false}, {true}});
    }

    @Parameterized.Parameter
    public boolean allMembersRouting;

    protected final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    private void assertNoLeftOver(Collection<Node> nodes, HecatonInstance client, UUID id,
                                  Collection<ClientConnectionRegistration> registrations) {
        assertTrueEventually(() -> {
            for (Node node : nodes) {
                assertNoLeftOverOnNode(node, registrations);
            }
            assertEquals(0, getClientEventRegistrations(client, id).size());
        });
    }

    private Collection<Node> createNodes() {
        int NODE_COUNT = 3;
        Collection<Node> nodes = new ArrayList<>(3);
        for (int i = 0; i < NODE_COUNT; i++) {
            HecatonInstance hecaton = hecatonFactory.newHecatonInstance();
            nodes.add(getNode(hecaton));
        }
        return nodes;
    }

    private void assertNoLeftOverOnNode(Node node, Collection<ClientConnectionRegistration> registrations) {
        Collection<ClientEndpoint> endpoints = node.clientEngine.getEndpointManager().getEndpoints();
        for (ClientEndpoint endpoint : endpoints) {
            for (ClientConnectionRegistration registration : registrations) {
                assertFalse(endpoint.removeDestroyAction(registration.getServerRegistrationId()));
            }
        }
    }

    private Collection<ClientConnectionRegistration> getClientEventRegistrations(HecatonInstance client, UUID id) {
        HecatonClientInstanceImpl clientImpl = ClientTestUtil.getHecatonClientInstanceImpl(client);
        ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) clientImpl.getListenerService();
        return listenerService.getActiveRegistrations(id).values();
    }

    private HecatonInstance newHecatonClient() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(allMembersRouting
                ? RoutingMode.ALL_MEMBERS : RoutingMode.SINGLE_MEMBER);
        return hecatonFactory.newHecatonClient(clientConfig);
    }

    @Test
    public void testMapEntryListeners() {
        Collection<Node> nodes = createNodes();
        HecatonInstance client = newHecatonClient();
        IMap map = client.getMap(randomString());
        UUID id = map.addEntryListener(mock(MapListener.class), false);

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(map.removeEntryListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testMapPartitionLostListeners() {
        Collection<Node> nodes = createNodes();
        HecatonInstance client = newHecatonClient();
        IMap map = client.getMap(randomString());
        UUID id = map.addPartitionLostListener(mock(MapPartitionLostListener.class));

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(map.removePartitionLostListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testMultiMapEntryListeners() {
        Collection<Node> nodes = createNodes();
        HecatonInstance client = newHecatonClient();
        MultiMap multiMap = client.getMultiMap(randomString());
        UUID id = multiMap.addEntryListener(mock(EntryListener.class), false);

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(multiMap.removeEntryListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testListListeners() {
        Collection<Node> nodes = createNodes();
        HecatonInstance client = newHecatonClient();
        IList<Object> list = client.getList(randomString());
        UUID id = list.addItemListener(mock(ItemListener.class), false);

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(list.removeItemListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testSetListeners() {
        Collection<Node> nodes = createNodes();
        HecatonInstance client = newHecatonClient();
        ISet<Object> set = client.getSet(randomString());
        UUID id = set.addItemListener(mock(ItemListener.class), false);

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(set.removeItemListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testQueueListeners() {
        Collection<Node> nodes = createNodes();
        HecatonInstance client = newHecatonClient();
        IQueue<Object> queue = client.getQueue(randomString());
        UUID id = queue.addItemListener(mock(ItemListener.class), false);

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(queue.removeItemListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testReplicatedMapListeners() {
        Collection<Node> nodes = createNodes();
        HecatonInstance client = newHecatonClient();
        ReplicatedMap<Object, Object> replicatedMap = client.getReplicatedMap(randomString());
        UUID id = replicatedMap.addEntryListener(mock(EntryListener.class));

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(replicatedMap.removeEntryListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testDistributedObjectListeners() {
        Collection<Node> nodes = createNodes();
        HecatonInstance client = newHecatonClient();
        UUID id = client.addDistributedObjectListener(mock(DistributedObjectListener.class));

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(client.removeDistributedObjectListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testTopicMessageListener() {
        Collection<Node> nodes = createNodes();
        HecatonInstance client = newHecatonClient();
        ITopic<Object> topic = client.getTopic(randomString());
        UUID id = topic.addMessageListener(mock(MessageListener.class));

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(topic.removeMessageListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testEventHandlersRemovedOnDisconnectedState() {
        //This test does not add any user listener because
        //we are testing if any event handler of internal listeners are leaking
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(allMembersRouting
                ? RoutingMode.ALL_MEMBERS : RoutingMode.SINGLE_MEMBER);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HecatonInstance hecaton = hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        hecaton.shutdown();

        assertTrueEventually(() -> assertEquals(0, getAllEventHandlers(client).size()));
    }

    @Test
    public void testListenerLeakOnMember_whenClientDestroyed() {
        Collection<Node> nodes = createNodes();

        for (int i = 0; i < 100; i++) {
            newHecatonClient().shutdown();
        }

        assertTrueEventually(() -> {
            for (Node node : nodes) {
                assertEquals(0, node.getClientEngine().getClusterViewListenerService().getClusterListeningEndpoints().size());
            }
        });
    }
}
