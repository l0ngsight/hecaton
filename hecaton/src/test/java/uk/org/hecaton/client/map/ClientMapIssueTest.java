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
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.query.PagingPredicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.NightlyTest;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapIssueTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testListenerRegistrations() {
        Config config = getConfig();
        ClientConfig clientConfig = getClientConfig();

        HecatonInstance instance1 = hecatonFactory.newHecatonInstance(config);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        final String mapName = randomMapName();
        IMap<Object, Object> map = client.getMap(mapName);
        map.addEntryListener(new EntryAdapter<>(), true);

        HecatonInstance instance2 = hecatonFactory.newHecatonInstance(config);

        instance1.getLifecycleService().terminate();
        instance1 = hecatonFactory.newHecatonInstance(config);

        final EventService eventService1 = getNodeEngineImpl(instance1).getEventService();
        final EventService eventService2 = getNodeEngineImpl(instance2).getEventService();

        assertTrueEventually(() -> {
            Collection<EventRegistration> regs1 = eventService1.getRegistrations(MapService.SERVICE_NAME, mapName);
            Collection<EventRegistration> regs2 = eventService2.getRegistrations(MapService.SERVICE_NAME, mapName);

            assertEquals("there should be only one registration", 1, regs1.size());
            assertEquals("there should be only one registration", 1, regs2.size());
        });
    }

    @Test
    public void testFutureGetCalledInCallback() {
        Config config = getConfig();
        ClientConfig clientConfig = getClientConfig();

        hecatonFactory.newHecatonInstance(config);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        IMap<Integer, Integer> map = client.getMap("map");
        final CompletionStage<Integer> future = map.getAsync(1);
        final CountDownLatch latch = new CountDownLatch(1);
        future.thenRun(latch::countDown);
        assertOpenEventually(latch);
    }

    @Test
    @Category(NightlyTest.class)
    public void testOperationNotBlockingAfterClusterShutdown() {
        Config config = getConfig();

        HecatonInstance instance1 = hecatonFactory.newHecatonInstance(config);
        HecatonInstance instance2 = hecatonFactory.newHecatonInstance(config);

        ClientConfig clientConfig = getClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "10");

        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        final IMap<String, String> map = client.getMap(randomMapName());

        map.put(randomString(), randomString());

        instance1.getLifecycleService().terminate();
        instance2.getLifecycleService().terminate();

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                map.get(randomString());
            } catch (Exception ignored) {
            } finally {
                latch.countDown();
            }
        }).start();

        assertOpenEventually(latch);
    }

    @Test
    @Category(NightlyTest.class)
    public void testOperationNotBlockingAfterClusterShutdown_withOneExecutorPoolSize() {
        Config config = getConfig();
        HecatonInstance instance1 = hecatonFactory.newHecatonInstance(config);
        HecatonInstance instance2 = hecatonFactory.newHecatonInstance(config);

        ClientConfig clientConfig = getClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "10");

        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        final IMap<String, String> map = client.getMap(randomMapName());

        map.put(randomString(), randomString());

        instance1.getLifecycleService().terminate();
        instance2.getLifecycleService().terminate();

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                map.get(randomString());
            } catch (Exception ignored) {
            } finally {
                latch.countDown();
            }
        }).start();

        assertOpenEventually(latch);
    }

    @Test
    public void testMapPagingEntries() {
        Config config = getConfig();
        hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);

        final ClientConfig clientConfig = getClientConfig();
        final HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        final IMap<Integer, Integer> map = client.getMap("map");

        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        final PagingPredicate<Integer, Integer> predicate = Predicates.pagingPredicate(pageSize);
        predicate.nextPage();

        final Set<Map.Entry<Integer, Integer>> entries = map.entrySet(predicate);
        assertEquals(pageSize, entries.size());
    }

    @Test
    public void testMapPagingValues() {
        Config config = getConfig();
        hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);

        final ClientConfig clientConfig = getClientConfig();
        final HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        final IMap<Integer, Integer> map = client.getMap("map");

        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        final PagingPredicate<Integer, Integer> predicate = Predicates.pagingPredicate(pageSize);
        predicate.nextPage();

        Collection<Integer> values = map.values(predicate);
        assertEquals(pageSize, values.size());

        /*
         * There may be cases when the server may return a list of entries larger than the requested page size, in this case
         * the client should not put any anchor into the list that is on a page greater than the requested page. The case occurs
         * when multiple members exist in the cluster. E.g.:
         * pageSize:5
         * page:2
         * Map entries are: (0,0), (1, 1), ... (50, 50)
         * server may return entries: 5, 6,7,8,9,10,13, 15, 16, 19 . We should not put (19, 19) as an anchor for page 2 but
         * only (9, 9) for page 1. The below code tests this issue.
         */
        // get the entries for page 0
        predicate.setPage(0);
        values = map.values(predicate);
        assertEquals(pageSize, values.size());
        int i = 0;
        for (Integer value : values) {
            assertEquals(i, value.intValue());
            ++i;
        }

        // get the entries for page 0, double check if calling second time works fine
        values = map.values(predicate);
        assertEquals(pageSize, values.size());
        i = 0;
        for (Integer value : values) {
            assertEquals(i, value.intValue());
            ++i;
        }

        // get page 1, predicate anchors should be updated accordingly
        predicate.nextPage();
        values = map.values(predicate);
        assertEquals(pageSize, values.size());
        i = pageSize;
        for (Integer value : values) {
            assertEquals(i, value.intValue());
            ++i;
        }

        // Make sure that the anchor is the last entry of the first page. i.e. it is (9, 9)
        Map.Entry anchor = predicate.getAnchor();
        assertEquals((2 * pageSize) - 1, anchor.getKey());
        assertEquals((2 * pageSize) - 1, anchor.getValue());

        // jump to page 4
        predicate.setPage(4);
        values = map.values(predicate);
        assertEquals(pageSize, values.size());
        i = 4 * pageSize;
        for (Integer value : values) {
            assertEquals(i, value.intValue());
            ++i;
        }

        // Make sure that the new predicate is page 4 and last entry is: (24, 24)
        anchor = predicate.getAnchor();
        assertEquals((5 * pageSize) - 1, anchor.getKey());
        assertEquals((5 * pageSize) - 1, anchor.getValue());

        // jump to page 9
        predicate.setPage(9);
        values = map.values(predicate);
        assertEquals(pageSize, values.size());
        i = 9 * pageSize;
        for (Integer value : values) {
            assertEquals(i, value.intValue());
            ++i;
        }

        // make sure that the anchor is now (10 * 5) -1 = (49, 49)
        anchor = predicate.getAnchor();
        assertEquals((10 * pageSize) - 1, anchor.getKey());
        assertEquals((10 * pageSize) - 1, anchor.getValue());
    }

    @Test
    public void testMapPagingKeySet() {
        Config config = getConfig();
        hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);

        final ClientConfig clientConfig = getClientConfig();
        final HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        final IMap<Integer, Integer> map = client.getMap("map");

        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(size - i, i);
        }

        final PagingPredicate<Integer, Integer> predicate = Predicates.pagingPredicate(pageSize);
        predicate.nextPage();

        final Set<Integer> values = map.keySet(predicate);
        assertEquals(pageSize, values.size());
    }

    @Test
    @Category(NightlyTest.class)
    public void testNoOperationTimeoutException_whenUserCodeLongRunning() {
        Config config = getConfig();
        long callTimeoutMillis = SECONDS.toMillis(10);
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), String.valueOf(callTimeoutMillis));
        hecatonFactory.newHecatonInstance(config);

        HecatonInstance client = hecatonFactory.newHecatonClient();
        IMap<Object, Object> map = client.getMap(randomMapName());
        SleepyProcessor sleepyProcessor = new SleepyProcessor(2 * callTimeoutMillis);
        String key = randomString();
        String value = randomString();
        map.put(key, value);
        assertEquals(value, map.executeOnKey(key, sleepyProcessor));
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    static class SleepyProcessor implements EntryProcessor, Serializable {

        private long millis;

        SleepyProcessor(long millis) {
            this.millis = millis;
        }

        @Override
        public Object process(Map.Entry entry) {
            try {

                Thread.sleep(millis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return entry.getValue();
        }

        @Override
        public EntryProcessor getBackupProcessor() {
            return null;
        }
    }
}
