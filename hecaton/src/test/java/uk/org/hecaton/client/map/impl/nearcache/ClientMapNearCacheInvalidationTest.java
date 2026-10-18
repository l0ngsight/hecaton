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

package uk.org.hecaton.client.map.impl.nearcache;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.proxy.ClientMapProxy;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.EntryView;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.nearcache.TestReadOnlyProcessor;
import uk.org.hecaton.nearcache.NearCacheStats;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static uk.org.hecaton.config.InMemoryFormat.OBJECT;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapNearCacheInvalidationTest extends ClientTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();
    private final String mapName = randomMapName();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testBatchInvalidationRemovesEntries() {
        Config config = getConfig();
        configureBatching(config, 10, 1);

        ClientConfig clientConfig = getClientConfig(mapName);

        HecatonInstance server = factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);

        HecatonInstance client = factory.newHecatonClient(clientConfig);
        makeSureConnectedToServers(client, 2);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        // fill Near Cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        // generate invalidation data
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        assertNearCacheSizeEventually(clientMap, 0);
    }

    @Test
    public void testHigherBatchSize_shouldNotCauseAnyInvalidation_onClient() {
        Config config = getConfig();
        configureBatching(config, Integer.MAX_VALUE, Integer.MAX_VALUE);

        ClientConfig clientConfig = getClientConfig(mapName);

        HecatonInstance server = factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);

        HecatonInstance client = factory.newHecatonClient(clientConfig);
        makeSureConnectedToServers(client, 2);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        // fill Near Cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        // generate invalidation data
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        assertNearCacheSizeEventually(clientMap, size);
    }

    @Test
    public void testMapClear_shouldClearNearCaches_onOwnerAndBackupNodes() {
        Config config = getConfig();
        configureBatching(config, 10, 1);

        ClientConfig clientConfig = getClientConfig(mapName);

        HecatonInstance server = factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);

        HecatonInstance client = factory.newHecatonClient(clientConfig);
        makeSureConnectedToServers(client, 2);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        // fill Near Cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        serverMap.clear();

        assertNearCacheSizeEventually(clientMap, 0);
    }

    @Test
    public void testPutAllWithMetadata_shouldClearNearCaches_onOwnerAndBackupNodes() {
        Config config = getConfig();
        configureBatching(config, 10, 1);

        ClientConfig clientConfig = getClientConfig(mapName);

        HecatonInstance server = factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);

        HecatonInstance client = factory.newHecatonClient(clientConfig);
        makeSureConnectedToServers(client, 2);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        List<EntryView<Integer, Integer>> entries = new ArrayList<>(size);

        // fill Near Cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
            entries.add(clientMap.getEntryView(i));
        }

        ((ClientMapProxy<Integer, Integer>) clientMap).putAllWithMetadataAsync(entries);

        assertNearCacheSizeEventually(clientMap, 0);
    }

    @Test
    public void testPutAll_shouldClearNearCaches_onOwnerAndBackupNodes() {
        Config config = getConfig();
        configureBatching(config, 10, 1);

        ClientConfig clientConfig = getClientConfig(mapName);

        HecatonInstance server = factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);

        HecatonInstance client = factory.newHecatonClient(clientConfig);
        makeSureConnectedToServers(client, 2);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        Map<Integer, Integer> entries = new HashMap<>(size);

        // fill Near Cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
            entries.put(i, i);
        }

        clientMap.putAll(entries);

        assertNearCacheSizeEventually(clientMap, 0);
    }

    @Test
    public void testMapSubmitToKey_withReadOnlyProcessor_noInvalidations() {
        verifyNoInvalidationsWith((map, size) -> {
            for (int i = 0; i < size; i++) {
                map.submitToKey(i, new TestReadOnlyProcessor());
            }
        });
    }

    @Test
    public void testMapExecuteOnKey_withReadOnlyProcessor_noInvalidations() {
        verifyNoInvalidationsWith((map, size) -> {
            for (int i = 0; i < size; i++) {
                map.executeOnKey(i, new TestReadOnlyProcessor());
            }
        });
    }

    @Test
    public void testMapSubmitToKeys_withReadOnlyProcessor_noInvalidations() {
        verifyNoInvalidationsWith((map, size) -> {
            Set<Integer> keys = IntStream.range(0, size).boxed().collect(Collectors.toSet());
            map.submitToKeys(keys, new TestReadOnlyProcessor());
        });
    }

    @Test
    public void testMapExecuteOnKeys_withReadOnlyProcessor_noInvalidations() {
        verifyNoInvalidationsWith((map, size) -> {
            Set<Integer> keys = IntStream.range(0, size).boxed().collect(Collectors.toSet());
            map.executeOnKeys(keys, new TestReadOnlyProcessor());
        });
    }

    @Test
    public void testMapExecuteOnEntriesWithPredicate_withReadOnlyProcessor_noInvalidations() {
        verifyNoInvalidationsWith((map, size) -> map.executeOnEntries(new TestReadOnlyProcessor(), Predicates.alwaysTrue()));
    }

    @Test
    public void testMapExecuteOnEntriesWithoutPredicate_withReadOnlyProcessor_noInvalidations() {
        verifyNoInvalidationsWith((map, size) -> map.executeOnEntries(new TestReadOnlyProcessor()));
    }

    @Test
    public void testMapEvictAll_shouldClearNearCaches_onOwnerAndBackupNodes() {
        Config config = getConfig();
        configureBatching(config, 10, 1);

        ClientConfig clientConfig = getClientConfig(mapName);

        HecatonInstance server = factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);

        HecatonInstance client = factory.newHecatonClient(clientConfig);
        makeSureConnectedToServers(client, 2);

        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 1000;

        // fill serverMap
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        // fill Near Cache on client
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        serverMap.evictAll();

        assertNearCacheSizeEventually(clientMap, 0);
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected ClientConfig getClientConfig(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);

        return new ClientConfig()
                .addNearCacheConfig(nearCacheConfig);
    }

    protected NearCacheConfig getNearCacheConfig(String mapName) {
        return new NearCacheConfig(mapName)
                    .setInMemoryFormat(OBJECT)
                    .setInvalidateOnChange(true);
    }

    private void verifyNoInvalidationsWith(BiConsumer<IMap<Integer, Integer>, Integer> operation) {
        Config config = getConfig();

        factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);

        ClientConfig clientConfig = getClientConfig(mapName);
        HecatonInstance client = factory.newHecatonClient(clientConfig);

        makeSureConnectedToServers(client, 2);

        int size = 100;

        IMap<Integer, Integer> map = client.getMap(mapName);

        for (int i = 0; i < size; i++) {
            map.put(i, i);
            map.get(i);
        }

        long invalidationsBefore = getInvalidationsFrom(map);

        operation.accept(map, size);

        assertTrueAllTheTime(() -> {
            long invalidationsAfter = getInvalidationsFrom(map);
            assertEquals(invalidationsBefore, invalidationsAfter);
        }, 3);
    }

    private static void configureBatching(Config config, int batchSize, int period) {
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true");
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), valueOf(batchSize));
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), valueOf(period));
    }

    private static long getInvalidationsFrom(IMap<Integer, Integer> map) {
        NearCacheStats stats = ((NearCachedClientMapProxy<Integer, Integer>) map).getNearCache().getNearCacheStats();
        return stats.getInvalidations();
    }

    private static void assertNearCacheSizeEventually(final IMap map, final int nearCacheSize) {
        assertTrueEventually(() -> {
            NearCache nearCache = ((NearCachedClientMapProxy) map).getNearCache();

            assertEquals(nearCacheSize, nearCache.size());
        });
    }
}
