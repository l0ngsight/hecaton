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
import uk.org.hecaton.client.config.ClientSecurityConfig;
import uk.org.hecaton.client.config.ConnectionRetryConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.impl.spi.impl.ClientExecutionServiceImpl;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.config.SerializationConfig;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapInterceptorAdaptor;
import uk.org.hecaton.map.listener.MapListener;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.nio.serialization.Portable;
import uk.org.hecaton.nio.serialization.PortableReader;
import uk.org.hecaton.nio.serialization.PortableWriter;
import uk.org.hecaton.security.UsernamePasswordCredentials;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.annotation.SlowTest;
import uk.org.hecaton.topic.ITopic;
import uk.org.hecaton.topic.MessageListener;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serial;
import java.util.Collection;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static uk.org.hecaton.config.MaxSizePolicy.ENTRY_COUNT;
import static uk.org.hecaton.core.LifecycleEvent.LifecycleState;
import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientRegressionWithMockNetworkTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testOperationRedo() {
        HecatonInstance hz1 = hecatonFactory.newHecatonInstance();
        hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            hz1.getLifecycleService().shutdown();
        });

        IMap<Integer, String> map = client.getMap("m");
        thread.start();
        int expected = 1000;
        for (int i = 0; i < expected; i++) {
            map.put(i, "item" + i);
        }
        assertJoinable(thread);
        assertEquals(expected, map.size());
    }

    @Test
    public void testOperationRedo_allMembersRoutingDisabled() {
        HecatonInstance hz1 = hecatonFactory.newHecatonInstance();
        hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            hz1.getLifecycleService().shutdown();
        });

        IMap<Integer, Integer> map = client.getMap("m");
        thread.start();
        int expected = 1000;
        for (int i = 0; i < expected; i++) {
            map.put(i, i);
        }
        assertJoinable(thread);
        assertEquals(expected, map.size());
    }

    @Test
    public void testGetDistributedObjectsIssue678() {
        HecatonInstance hz = hecatonFactory.newHecatonInstance();
        hz.getQueue("queue");
        hz.getMap("map");
        hz.getList("list");
        HecatonInstance instance = hecatonFactory.newHecatonClient();
        Collection<DistributedObject> distributedObjects = instance.getDistributedObjects();
        assertEquals(3, distributedObjects.size());
    }

    @Test
    public void testMapDestroyIssue764() {
        HecatonInstance server = hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();
        assertNoOfDistributedObject("Initially the server should have %d distributed objects, but had %d", 0, server.getDistributedObjects());
        assertNoOfDistributedObject("Initially the client should have %d distributed objects, but had %d", 0, client.getDistributedObjects());

        IMap<Object, Object> map = client.getMap("mapToDestroy");
        assertNoOfDistributedObject("After getMap() the server should have %d distributed objects, but had %d", 1, server.getDistributedObjects());
        assertNoOfDistributedObject("After getMap() the client should have %d distributed objects, but had %d", 1, client.getDistributedObjects());

        map.destroy();
        // Get the distributed objects as fast as possible to catch a race condition more likely
        Collection<DistributedObject> serverDistributedObjects = server.getDistributedObjects();
        Collection<DistributedObject> clientDistributedObjects = client.getDistributedObjects();
        assertNoOfDistributedObject("After destroy() the server should should have %d distributed objects, but had %d", 0, serverDistributedObjects);
        assertNoOfDistributedObject("After destroy() the client should should have %d distributed objects, but had %d", 0, clientDistributedObjects);
    }

    private void assertNoOfDistributedObject(String message, int expected, Collection<DistributedObject> distributedObjects) {
        StringBuilder sb = new StringBuilder(message + "\n");
        for (DistributedObject distributedObject : distributedObjects) {
            sb
                    .append("Name: ").append(distributedObject.getName())
                    .append(", Service: ").append(distributedObject.getServiceName())
                    .append(", PartitionKey: ").append(distributedObject.getPartitionKey())
                    .append("\n");
        }
        assertEqualsStringFormat(sb.toString(), expected, distributedObjects.size());
    }

    /**
     * Client hangs at map.get after shutdown
     */
    @Test
    public void testIssue821() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE));
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(5000);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        IMap<Object, Object> map = client.getMap("default");

        map.put("key1", "value1");

        instance.shutdown();

        try {
            map.get("key1");
            fail();
        } catch (Exception ignored) {
        }
        assertFalse(instance.getLifecycleService().isRunning());
    }

    @Test
    public void testClientConnectionEvents() {
        LinkedList<LifecycleState> list = new LinkedList<>();
        list.offer(LifecycleState.STARTING);
        list.offer(LifecycleState.STARTED);
        list.offer(LifecycleState.SHUTTING_DOWN);
        list.offer(LifecycleState.SHUTDOWN);

        //to make sure event CONNECT/DISCONNECT order is correct
        AtomicReference<Object> stateAtomicReference = new AtomicReference<>();
        stateAtomicReference.set(CLIENT_DISCONNECTED);
        Object FAILURE = new Object();

        hecatonFactory.newHecatonInstance();
        CountDownLatch latch = new CountDownLatch(list.size());
        CountDownLatch connectedLatch = new CountDownLatch(2);
        CountDownLatch disconnectedLatch = new CountDownLatch(2);
        LifecycleListener listener = new LifecycleListener() {
            public void stateChanged(LifecycleEvent event) {
                Logger.getLogger(getClass()).info("stateChanged: " + event);
                LifecycleState eventState = event.getState();

                if (CLIENT_CONNECTED == eventState) {
                    if (!stateAtomicReference.compareAndSet(CLIENT_DISCONNECTED, CLIENT_CONNECTED)) {
                        //fail the test
                        stateAtomicReference.set(FAILURE);
                    }
                    connectedLatch.countDown();
                } else if (CLIENT_DISCONNECTED == eventState) {
                    if (!stateAtomicReference.compareAndSet(CLIENT_CONNECTED, CLIENT_DISCONNECTED)) {
                        //fail the test
                        stateAtomicReference.set(FAILURE);
                    }
                    disconnectedLatch.countDown();
                } else {
                    LifecycleState state = list.poll();
                    if (state != null && state == eventState) {
                        latch.countDown();
                    }
                }
            }
        };
        ListenerConfig listenerConfig = new ListenerConfig(listener);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(listenerConfig);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HecatonInstance hecatonClient = hecatonFactory.newHecatonClient(clientConfig);

        hecatonFactory.shutdownAllMembers();

        hecatonFactory.newHecatonInstance();

        assertOpenEventually("Expected at least two CLIENT_CONNECTED events!", connectedLatch);

        hecatonFactory.shutdownAllMembers();

        //wait for disconnect then call client.shutdown(). Otherwise shutdown could prevent firing DISCONNECTED event
        assertOpenEventually("Expected at least two CLIENT_DISCONNECTED events!", disconnectedLatch);

        hecatonClient.shutdown();

        assertOpenEventually("LifecycleState failed", latch);

        assertNotEquals("CLIENT_CONNECTED CLIENT_DISCONNECTED order was broken ", FAILURE, stateAtomicReference.get());
    }

    @Test
    public void testInterceptor() {
        hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();

        IMap<Object, Object> map = client.getMap("map");
        MapInterceptorImpl interceptor = new MapInterceptorImpl();

        String id = map.addInterceptor(interceptor);
        assertNotNull(id);

        map.put("key1", "value");
        assertEquals("value", map.get("key1"));
        map.put("key1", "value1");
        assertEquals("getIntercepted", map.get("key1"));

        assertFalse(map.replace("key1", "getIntercepted", "val"));
        assertTrue(map.replace("key1", "value1", "val"));

        assertEquals("val", map.get("key1"));

        map.put("key2", "oldValue");
        assertEquals("oldValue", map.get("key2"));
        map.put("key2", "newValue");
        assertEquals("putIntercepted", map.get("key2"));

        map.put("key3", "value2");
        assertEquals("value2", map.get("key3"));
        assertEquals("removeIntercepted", map.remove("key3"));
    }

    private static class MapInterceptorImpl extends MapInterceptorAdaptor {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public Object interceptGet(Object value) {
            if ("value1".equals(value)) {
                return "getIntercepted";
            }
            return super.interceptGet(value);
        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            if ("oldValue".equals(oldValue) && "newValue".equals(newValue)) {
                return "putIntercepted";
            }
            return super.interceptPut(oldValue, newValue);
        }

        @Override
        public Object interceptRemove(Object removedValue) {
            if ("value2".equals(removedValue)) {
                return "removeIntercepted";
            }
            return super.interceptRemove(removedValue);
        }
    }

    @Test
    public void testClientPortableWithoutRegisteringToNode() {
        hecatonFactory.newHecatonInstance();
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(5, classId -> new SamplePortable());
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setSerializationConfig(serializationConfig);

        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        IMap<Integer, SamplePortable> sampleMap = client.getMap(randomString());
        sampleMap.put(1, new SamplePortable(666));
        SamplePortable samplePortable = sampleMap.get(1);
        assertEquals(666, samplePortable.a);
    }

    @Test
    public void testCredentials() {
        Config config = new Config();
        config.setClusterName("foo");
        hecatonFactory.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("foo");
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(1000);
        ClientSecurityConfig securityConfig = clientConfig.getSecurityConfig();
        securityConfig.setCredentials(new MyCredentials());
        // not null username/password credentials are not allowed when Security is disabled
        assertThrows(IllegalStateException.class, () -> hecatonFactory.newHecatonClient(clientConfig));
    }

    public static class MyCredentials extends UsernamePasswordCredentials {

        public MyCredentials() {
            super("foo", "bar");
        }
    }

    public void testListenerReconnect() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();

        CountDownLatch latch = new CountDownLatch(2);

        IMap<Object, Object> m = client.getMap("m");
        UUID id = m.addEntryListener(new EntryAdapter<>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                latch.countDown();
            }

            @Override
            public void entryUpdated(EntryEvent<Object, Object> event) {
                latch.countDown();
            }
        }, true);


        m.put("key1", "value1");

        hecatonFactory.newHecatonInstance();
        instance.shutdown();

        Thread thread = new Thread() {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    m.put("key2", "value2");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        };
        thread.start();

        assertOpenEventually(latch, 10);
        thread.interrupt();
        assertJoinable(thread);
        assertTrue(m.removeEntryListener(id));
        assertFalse(m.removeEntryListener(UUID.randomUUID()));
    }

    static class SamplePortable implements Portable {

        public int a;

        SamplePortable(int a) {
            this.a = a;
        }

        SamplePortable() {
        }

        @Override
        public int getFactoryId() {
            return 5;
        }

        @Override
        public int getClassId() {
            return 6;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("a", a);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            a = reader.readInt("a");
        }
    }

    @Test
    public void testNearCache_WhenRegisteredNodeIsDead() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        String mapName = randomMapName();

        NearCacheConfig nearCacheConfig = new NearCacheConfig();

        nearCacheConfig.setName(mapName);
        nearCacheConfig.setInvalidateOnChange(true);
        clientConfig.addNearCacheConfig(nearCacheConfig);

        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        IMap<Object, Object> map = client.getMap(mapName);

        map.put("a", "b");
        // populate Near Cache
        map.get("a");

        instance.shutdown();
        hecatonFactory.newHecatonInstance();

        assertTrueEventually(() -> assertNull(map.get("a")));
    }

    @Test
    public void testDeadlock_WhenDoingOperationFromListeners() {
        hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient(new ClientConfig());

        int putCount = 1000;

        CountDownLatch latch = new CountDownLatch(putCount);

        IMap<Object, Object> map1 = client.getMap(randomMapName());
        IMap<Object, Object> map2 = client.getMap(randomMapName());

        map1.addEntryListener(new EntryAdapter<>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                map2.put(1, 1);
                latch.countDown();
            }
        }, false);

        for (int i = 0; i < putCount; i++) {
            map1.put(i, i);
        }

        assertOpenEventually(latch);
    }

    @Test
    public void testDeadlock_WhenDoingOperationFromLifecycleListener() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch finishedLatch = new CountDownLatch(1);
        IMap<Object, Object> map = client.getMap(randomMapName());

        client.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == CLIENT_DISCONNECTED) {
                disconnectedLatch.countDown();
                for (int i = 0; i < 1000; i++) {
                    map.get(i);
                }
                finishedLatch.countDown();
            }
        });

        instance.shutdown();
        assertOpenEventually(disconnectedLatch);
        hecatonFactory.newHecatonInstance();
        assertOpenEventually(finishedLatch);
    }

    @Test
    public void testDeadlock_WhenDoingOperationFromLifecycleListenerWithInitialPartitionTable() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch finishedLatch = new CountDownLatch(1);
        IMap<Object, Object> map = client.getMap(randomMapName());

        // Let the partition table retrieved the first time
        map.get(1);

        client.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == CLIENT_DISCONNECTED) {
                disconnectedLatch.countDown();
                for (int i = 0; i < 1000; i++) {
                    map.get(i);
                }
                finishedLatch.countDown();
            }
        });

        instance.shutdown();
        assertOpenEventually(disconnectedLatch);
        hecatonFactory.newHecatonInstance();
        assertOpenEventually(finishedLatch);
    }

    @Test
    public void testDeadlock_whenDoingOperationFromLifecycleListener_withNearCache() {
        String mapName = randomMapName();

        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(1);

        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setName(mapName)
                .setEvictionConfig(evictionConfig);

        ClientConfig clientConfig = new ClientConfig()
                .addNearCacheConfig(nearCacheConfig);

        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch finishedLatch = new CountDownLatch(1);
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        IMap<Object, Object> map = client.getMap(mapName);

        client.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == CLIENT_DISCONNECTED) {
                disconnectedLatch.countDown();
                map.get(1);
                map.get(2);
                finishedLatch.countDown();
            }
        });

        instance.shutdown();
        assertOpenEventually(disconnectedLatch);
        hecatonFactory.newHecatonInstance();
        assertOpenEventually(finishedLatch);
    }

    @Test(expected = ExecutionException.class, timeout = 120000)
    public void testGithubIssue3557() throws Exception {
        HecatonInstance hz = hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();

        UnDeserializable unDeserializable = new UnDeserializable(1);
        IExecutorService executorService = client.getExecutorService("default");
        Issue2509Runnable task = new Issue2509Runnable(unDeserializable);
        Future<?> future = executorService.submitToMember(task, hz.getCluster().getLocalMember());
        future.get();
    }

    public static class Issue2509Runnable implements Callable<Integer>, DataSerializable {

        private UnDeserializable unDeserializable;

        public Issue2509Runnable() {
        }

        public Issue2509Runnable(UnDeserializable unDeserializable) {
            this.unDeserializable = unDeserializable;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(unDeserializable);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            unDeserializable = in.readObject();
        }

        @Override
        public Integer call() {
            return unDeserializable.foo;
        }
    }

    public static class UnDeserializable implements DataSerializable {

        private int foo;

        public UnDeserializable(int foo) {
            this.foo = foo;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(foo);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            foo = in.readInt();
        }
    }

    @Test
    public void testNearCache_shutdownClient() {
        ClientConfig clientConfig = new ClientConfig();
        NearCacheConfig invalidateConfig = new NearCacheConfig();
        String mapName = randomMapName();
        invalidateConfig.setName(mapName);
        invalidateConfig.setInvalidateOnChange(true);
        clientConfig.addNearCacheConfig(invalidateConfig);
        hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        IMap<Integer, Integer> map = client.getMap(mapName);

        map.get(1);
        //test should finish without throwing any exception.
        client.shutdown();
    }

    @Test
    public void testClientReconnect_thenCheckRequestsAreRetriedWithoutException() {
        HecatonInstance hecatonInstance = hecatonFactory.newHecatonInstance();

        CountDownLatch clientStartedDoingRequests = new CountDownLatch(1);
        Thread restartThread = new Thread(() -> {
            try {
                clientStartedDoingRequests.await();
            } catch (InterruptedException ignored) {
            }

            hecatonInstance.shutdown();

            hecatonFactory.newHecatonInstance();
        });
        restartThread.start();

        ClientConfig clientConfig = new ClientConfig();
        //Retry all requests
        clientConfig.getNetworkConfig().setRedoOperation(true);
        //retry to connect to cluster forever(never shutdown the client)
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        //Retry all requests forever(until client is shutdown)
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE));
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        IMap<Object, Object> map = client.getMap(randomMapName());

        int mapSize = 1000;
        for (int i = 0; i < mapSize; i++) {
            if (i == mapSize / 4) {
                clientStartedDoingRequests.countDown();
            }
            try {
                map.put(i, i);
            } catch (Exception e) {
                fail("Requests should not throw exception with this configuration. Last put key: " + i);
            }
        }
        assertJoinable(restartThread);
    }

    @Test
    public void testClusterShutdown_thenCheckOperationsNotHanging() throws Exception {
        HecatonInstance hecatonInstance = hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        //Retry all requests
        clientConfig.getNetworkConfig().setRedoOperation(true);
        //Retry all requests forever(until client is shutdown)
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE));
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(5000);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        IMap<Object, Object> map = client.getMap(randomMapName());
        int mapSize = 1000;

        CountDownLatch clientStartedDoingRequests = new CountDownLatch(1);
        int threadCount = 100;

        CountDownLatch testFinishedLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(() -> {
                try {
                    for (int i1 = 0; i1 < mapSize; i1++) {
                        if (i1 == mapSize / 4) {
                            clientStartedDoingRequests.countDown();
                        }

                        map.put(i1, i1);

                    }
                } catch (Throwable ignored) {
                } finally {
                    testFinishedLatch.countDown();
                }
            });
            thread.start();
        }

        assertTrue(clientStartedDoingRequests.await(30, TimeUnit.SECONDS));

        hecatonInstance.shutdown();

        assertOpenEventually("Put operations should not hang.", testFinishedLatch);
    }

    @Test(timeout = 120000)
    public void testMemberAddedWithListeners_thenCheckOperationsNotHanging() {
        hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientExecutionServiceImpl.INTERNAL_EXECUTOR_POOL_SIZE.getName(), "1");
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        IMap<Object, Object> map = client.getMap("map");
        map.addEntryListener(mock(MapListener.class), true);
        HecatonInstance h2 = hecatonFactory.newHecatonInstance();
        String key = generateKeyOwnedBy(h2);
        map.get(key);
    }


    @Test
    @Category(SlowTest.class)
    public void testServerShouldNotCloseClientWhenClientOnlyListening() {
        Config config = new Config();
        int clientHeartbeatSeconds = 8;
        config.setProperty(ClusterProperty.CLIENT_HEARTBEAT_TIMEOUT_SECONDS.getName(), String.valueOf(clientHeartbeatSeconds));
        hecatonFactory.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.HEARTBEAT_INTERVAL.getName(), "1000");
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        HecatonInstance client2 = hecatonFactory.newHecatonClient();

        AtomicBoolean isClientDisconnected = new AtomicBoolean();
        client.getLifecycleService().addLifecycleListener(event -> {
            if (CLIENT_DISCONNECTED == event.getState()) {
                isClientDisconnected.set(true);
            }
        });

        String key = "topicName";
        ITopic<Object> topic = client.getTopic(key);
        MessageListener<Object> listener = message -> {
        };
        UUID id = topic.addMessageListener(listener);

        ITopic<Object> client2Topic = client2.getTopic(key);
        long begin = System.currentTimeMillis();

        while (System.currentTimeMillis() - begin < TimeUnit.SECONDS.toMillis(clientHeartbeatSeconds * 2)) {
            client2Topic.publish("message");
        }

        topic.removeMessageListener(id);
        assertFalse(isClientDisconnected.get());
    }

    @Test(timeout = 20000)
    public void testClientShutdown_shouldNotWaitForNextConnectionAttempt() {
        HecatonInstance server = hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        ConnectionRetryConfig connectionRetryConfig = clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig();
        connectionRetryConfig.setInitialBackoffMillis(240000);
        connectionRetryConfig.setMaxBackoffMillis(300000);
        connectionRetryConfig.setMultiplier(1);
        connectionRetryConfig.setClusterConnectTimeoutMillis(Integer.MAX_VALUE);


        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        //client will get into retry
        server.shutdown();
        // we expect this shutdown not to wait for 4 minutes (240000).
        // Test will time out in 20 seconds. Note that global executor shutdown in ClientExecutionServiceImpl timeout is 30 seconds
        client.shutdown();
    }
}
