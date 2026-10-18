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
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapStore;
import uk.org.hecaton.map.impl.mapstore.writebehind.MapStoreWithCounter;
import uk.org.hecaton.map.impl.mapstore.writebehind.TemporaryBlockerMapStore;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static uk.org.hecaton.map.impl.mapstore.writebehind.WriteBehindOnBackupsTest.writeBehindQueueSize;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("WeakerAccess")
public class ClientWriteBehindFlushTest extends HecatonTestSupport {

    private static final String MAP_NAME = "default";

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.shutdownAll();
    }

    @Test
    public void testWriteBehindQueues_emptied_onOwnerAndBackupNodes() {
        MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setImplementation(mapStore)
                .setWriteDelaySeconds(3000);

        Config config = getConfig();
        config.getMapConfig(MAP_NAME).setMapStoreConfig(mapStoreConfig);

        HecatonInstance member1 = hecatonFactory.newHecatonInstance(config);
        HecatonInstance member2 = hecatonFactory.newHecatonInstance(config);
        HecatonInstance member3 = hecatonFactory.newHecatonInstance(config);

        HecatonInstance client = hecatonFactory.newHecatonClient(getClientConfig());
        IMap<Integer, Integer> map = client.getMap(MAP_NAME);

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        map.flush();

        assertWriteBehindQueuesEmpty(MAP_NAME, asList(member1, member2, member3));
    }

    @Test
    public void testFlush_shouldNotCause_concurrentStoreOperation() {
        int blockStoreOperationSeconds = 5;
        TemporaryBlockerMapStore store = new TemporaryBlockerMapStore(blockStoreOperationSeconds);
        Config config = newMapStoredConfig(store, 2);

        hecatonFactory.newHecatonInstance(config);

        IMap<String, String> map = hecatonFactory.newHecatonClient(getClientConfig()).getMap(MAP_NAME);

        map.put("key", "value");
        map.flush();

        assertEquals("Expecting only one store after flush", 1, store.getStoreOperationCount());
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    @SuppressWarnings("SameParameterValue")
    protected Config newMapStoredConfig(MapStore store, int writeDelaySeconds) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setWriteDelaySeconds(writeDelaySeconds)
                .setImplementation(store);

        Config config = getConfig();
        config.getMapConfig(MAP_NAME)
                .setMapStoreConfig(mapStoreConfig);

        return config;
    }

    protected void assertWriteBehindQueuesEmpty(final String mapName, final List<HecatonInstance> nodes) {
        assertTrueEventually(() -> {
            for (HecatonInstance instance : nodes) {
                assertEquals(0, writeBehindQueueSize(instance, mapName));
            }
        });
    }
}
