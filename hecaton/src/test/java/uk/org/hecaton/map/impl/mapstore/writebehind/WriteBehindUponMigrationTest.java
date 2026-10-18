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

package uk.org.hecaton.map.impl.mapstore.writebehind;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapStore;
import uk.org.hecaton.map.impl.mapstore.MapStoreTest;
import uk.org.hecaton.map.impl.mapstore.TestEntryStore;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.instance.impl.TestUtil.terminateInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class WriteBehindUponMigrationTest extends HecatonTestSupport {

    @Test
    public void testRemovedEntry_shouldNotBeReached_afterMigration_entryStore() {
        TestEntryStore<Integer, Integer> store = new TestEntryStore<>();
        store.putExternally(1, 0);
        testRemovedEntry_shouldNotBeReached_afterMigration(store);
    }

    @Test
    public void testEntryStoreShouldExpireEntryTimely_afterMigration() {
        TemporaryBlockerEntryStore<Integer, Integer> store = new TemporaryBlockerEntryStore<>();
        String mapName = randomMapName();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);

        Config config = createConfig(mapName, store, 5);
        HecatonInstance node1 = factory.newHecatonInstance(config);

        IMap<Integer, Integer> map = node1.getMap(mapName);

        map.put(1, 1, 30, TimeUnit.SECONDS);
        long expectedExpirationTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        HecatonInstance node2 = factory.newHecatonInstance(config);
        waitClusterForSafeState(node1);

        terminateInstance(node1);

        map = node2.getMap(mapName);

        store.storePermit.release();

        map.evictAll();

        assertTrueEventually(() -> store.assertRecordStored(1, 1, expectedExpirationTime, 10000), 20);
        assertEquals(1, (int) map.get(1));
        sleepAtLeastSeconds(30);
        assertNull(map.get(1));
    }

    @Test
    public void testRemovedEntry_shouldNotBeReached_afterMigration_mapStore() {
        MapStoreTest.SimpleMapStore<Integer, Integer> store
                = new MapStoreTest.SimpleMapStore<>();
        store.store.put(1, 0);
        testRemovedEntry_shouldNotBeReached_afterMigration(store);
    }

    public void testRemovedEntry_shouldNotBeReached_afterMigration(MapStore store) {
        String mapName = randomMapName();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);


        Config config = createConfig(mapName, store, 100);
        HecatonInstance node1 = factory.newHecatonInstance(config);

        IMap<Integer, Integer> map = node1.getMap(mapName);

        map.put(1, 1);
        map.delete(1);

        HecatonInstance node2 = factory.newHecatonInstance(config);

        map = node2.getMap(mapName);

        Integer value = map.get(1);

        assertNull(value);
    }

    private static Config createConfig(String mapName, MapStore store, int writeDelaySeconds) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig
                .setImplementation(store)
                .setWriteDelaySeconds(writeDelaySeconds)
                .setWriteBatchSize(1)
                .setWriteCoalescing(false);

        Config config = new Config();
        config.getMapConfig(mapName)
                .setBackupCount(1)
                .setMapStoreConfig(mapStoreConfig);

        return config;
    }
}
