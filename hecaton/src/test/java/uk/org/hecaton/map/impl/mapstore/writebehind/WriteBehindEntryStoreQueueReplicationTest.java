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
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.EntryStore;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.mapstore.TestEntryStore;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteBehindEntryStoreQueueReplicationTest extends HecatonTestSupport {

    @Test
    public void queued_entries_with_expiration_times_are_not_lost_when_cluster_scaled_down() {
        final int entryCount = 1000;
        final int ttlSec = 10;
        final int writeDelaySec = 5;
        final int backupCount = 1;
        final String mapName = randomMapName();

        TestEntryStore<Integer, Integer> testEntryStore = new TestEntryStore<>();
        Config config = getConfigWithEntryStore(testEntryStore, writeDelaySec, backupCount);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance[] instances = factory.newInstances(config);

        Map<Integer, Long> expectedExpiryTimes = new HashMap<>();
        IMap<Integer, Integer> map = instances[0].getMap(mapName);
        for (int i = 0; i < 1000; i++) {
            map.put(i, i, ttlSec, TimeUnit.SECONDS);
            expectedExpiryTimes.put(i, map.getEntryView(i).getExpirationTime());
        }

        // scale down
        instances[0].shutdown();
        instances[1].shutdown();

        IMap<Integer, Integer> mapFromSurvivingInstance = instances[2].getMap(mapName);

        // assert that entries are still stored in
        // the entry store after original nodes crash
        assertTrueEventually(() -> {
            for (int i = 0; i < entryCount; i++) {
                testEntryStore.assertRecordStored(i, i, expectedExpiryTimes.get(i), 2000);
            }
        });

        // assert that entries still had
        // expiration times and expired accordingly
        assertTrueEventually(() -> {
            for (int i = 0; i < entryCount; i++) {
                assertNull(dumpNotExpiredRecordsToString(instances[2], mapName),
                        mapFromSurvivingInstance.get(i));
            }
        }, 240);
    }

    private static String dumpNotExpiredRecordsToString(HecatonInstance node, String mapName) {
        List<Long> msg = new ArrayList<>();
        NodeEngineImpl nodeEngine = getNode(node).getNodeEngine();
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            RecordStore<Record> recordStore = mapServiceContext.getExistingRecordStore(i, mapName);
            if (recordStore == null) {
                continue;
            }
            recordStore.forEach((data, record) -> msg.add(recordStore.getExpirySystem()
                    .getExpiryMetadata(data).getTtl()), false);
        }
        return msg.toString();
    }

    @Test
    public void queued_entries_with_expirationTimes_are_replicated_when_cluster_scaled_up() {
        final int entryCount = 1000;
        final int ttlSec = 10;
        final int writeDelaySec = 5;
        final int backupCount = 1;
        final String mapName = randomMapName();

        TestEntryStore<Integer, Integer> testEntryStore = new TestEntryStore<>();
        Config config = getConfigWithEntryStore(testEntryStore, writeDelaySec, backupCount);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance node1 = factory.newHecatonInstance(config);

        Map<Integer, Long> expectedExpiryTimes = new HashMap<>();
        IMap<Integer, Integer> map = node1.getMap(mapName);
        for (int i = 0; i < 1000; i++) {
            map.put(i, i, ttlSec, TimeUnit.SECONDS);
            expectedExpiryTimes.put(i, map.getEntryView(i).getExpirationTime());
        }

        // scale up
        HecatonInstance node2 = factory.newHecatonInstance(config);
        HecatonInstance node3 = factory.newHecatonInstance(config);

        IMap<Integer, Integer> mapFromNewInstance = node3.getMap(mapName);

        // assert that entries are still stored in
        // the entry store after original nodes crash
        assertTrueEventually(() -> {
            for (int i = 0; i < entryCount; i++) {
                testEntryStore.assertRecordStored(i, i, expectedExpiryTimes.get(i), 2000);
            }
        });

        // assert that entries still had
        // expiration times and expired accordingly
        assertTrueEventually(() -> {
            for (int i = 0; i < entryCount; i++) {
                assertNull(dumpNotExpiredRecordsToString(node3, mapName),
                        mapFromNewInstance.get(i));
            }
        }, 240);
    }

    private Config getConfigWithEntryStore(EntryStore entryStore,
                                           int writeDelaySeconds, int backupCount) {
        Config config = getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig
                .setEnabled(true)
                .setWriteDelaySeconds(writeDelaySeconds)
                .setImplementation(entryStore);
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.setBackupCount(backupCount);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        return config;
    }
}
