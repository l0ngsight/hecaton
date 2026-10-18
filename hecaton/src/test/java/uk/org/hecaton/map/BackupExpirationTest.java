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

package uk.org.hecaton.map;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.EntryView;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.internal.nearcache.impl.invalidation.InvalidationQueue;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static uk.org.hecaton.config.InMemoryFormat.BINARY;
import static uk.org.hecaton.config.InMemoryFormat.OBJECT;
import static uk.org.hecaton.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BackupExpirationTest extends HecatonTestSupport {

    private static final String MAP_NAME = "test";
    private static final int NODE_COUNT = 3;

    private HecatonInstance[] nodes;

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameters(name = "inMemoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY}, {OBJECT},
        });
    }

    protected void configureAndStartNodes(int maxIdleSeconds, int partitionCount,
                                          int expirationTaskPeriodSeconds) {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(NODE_COUNT);

        Config config = getConfig();
        config.setProperty(PARTITION_COUNT.getName(), Integer.toString(partitionCount));
        config.setProperty(PROP_TASK_PERIOD_SECONDS, Integer.toString(expirationTaskPeriodSeconds));
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setBackupCount(NODE_COUNT - 1);
        mapConfig.setMaxIdleSeconds(maxIdleSeconds);
        mapConfig.setInMemoryFormat(inMemoryFormat);

        nodes = factory.newInstances(config);
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void all_backups_should_be_empty_eventually() {
        configureAndStartNodes(3, 11, 1);

        IMap map = nodes[0].getMap(MAP_NAME);
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        assertTrueEventually(() -> {
            for (HecatonInstance node : nodes) {
                assertEquals(0, getTotalEntryCount(node.getMap(MAP_NAME).getLocalMapStats()));
            }
        });
    }

    @Test
    public void updates_on_same_key_prevents_expiration_on_backups() {
        configureAndStartNodes(10, 1, 1);
        long waitTimeInMillis = 1000;

        IMap map = nodes[0].getMap(MAP_NAME);
        map.put(1, 1);

        final BackupExpiryTimeReader backupExpiryTimeReader = new BackupExpiryTimeReader(MAP_NAME);

        // First call to read expiry time
        map.executeOnKey(1, backupExpiryTimeReader);

        sleepAtLeastMillis(waitTimeInMillis);
        map.put(1, 1);

        // Second call to read expiry time
        map.executeOnKey(1, backupExpiryTimeReader);

        final int backupCount = NODE_COUNT - 1;
        final int executeOnKeyCallCount = 2;

        assertTrueEventually(() -> assertEquals(executeOnKeyCallCount * backupCount,
                ExpiryTimeReader.TIMES_QUEUE.size()));

        long expiryFoundAt1stCall = -1;
        for (int i = 0; i < backupCount; i++) {
            expiryFoundAt1stCall = ExpiryTimeReader.TIMES_QUEUE.poll();
        }

        long expiryFoundAt2ndCall = -1;
        for (int i = 0; i < backupCount; i++) {
            expiryFoundAt2ndCall = ExpiryTimeReader.TIMES_QUEUE.poll();
        }


        assertTrue(expiryFoundAt2ndCall + "-" + expiryFoundAt1stCall,
                expiryFoundAt2ndCall >= expiryFoundAt1stCall + waitTimeInMillis);
    }


    public static class BackupExpiryTimeReader
            implements EntryProcessor<Integer, Integer, Object>, Serializable {

        private String mapName;

        public BackupExpiryTimeReader(String mapName) {
            this.mapName = mapName;
        }

        @Override
        public Object process(Map.Entry<Integer, Integer> entry) {
            return null;
        }

        @Override
        public EntryProcessor<Integer, Integer, Object> getBackupProcessor() {
            return new ExpiryTimeReader(mapName);
        }
    }

    public static class ExpiryTimeReader
            implements EntryProcessor<Integer, Integer, Object>, HecatonInstanceAware, Serializable {

        public static final ConcurrentLinkedQueue<Long> TIMES_QUEUE = new ConcurrentLinkedQueue<>();

        private transient HecatonInstance instance;

        private String mapName;

        public ExpiryTimeReader(String mapName) {
            this.mapName = mapName;
        }

        @Override
        public Object process(Map.Entry<Integer, Integer> entry) {
            EntryView entryView = instance.getMap(mapName).getEntryView(entry.getKey());

            TIMES_QUEUE.add(entryView.getExpirationTime());
            return null;
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            this.instance = hecatonInstance;
        }
    }

    @Test
    public void dont_collect_expired_keys_if_expiration_reason_is_TTL() {
        configureAndStartNodes(30, 1, 5);

        IMap map = nodes[0].getMap(MAP_NAME);
        Object expirationQueueLength = map.executeOnKey("1", new BackupExpirationQueueLengthFinder());

        assertEquals(0, ((Integer) expirationQueueLength).intValue());
    }

    // This EP mimics TTL expiration process by creating a record
    // which expires after 100 millis. TTL expired key should not be added to the expiration queue
    // after `recordStore.get`.
    public static final class BackupExpirationQueueLengthFinder
            implements HecatonInstanceAware, EntryProcessor {

        private transient HecatonInstance node;

        @Override
        public Object process(Map.Entry entry) {
            NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(node);
            SerializationService ss = nodeEngineImpl.getSerializationService();
            MapService mapService = nodeEngineImpl.getService(MapService.SERVICE_NAME);
            MapServiceContext mapServiceContext = mapService.getMapServiceContext();
            RecordStore recordStore = mapServiceContext.getRecordStore(0, MAP_NAME);

            Data dataKey = ss.toData(1);
            recordStore.put(dataKey, "value", 100, -1);
            sleepSeconds(1);
            recordStore.get(dataKey, false, null);

            InvalidationQueue expiredKeys = recordStore.getExpiredKeysQueue();
            return expiredKeys.size();
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            this.node = hecatonInstance;
        }
    }

    public static long getTotalEntryCount(LocalMapStats localMapStats) {
        long ownedEntryCount = localMapStats.getOwnedEntryCount();
        long backupEntryCount = localMapStats.getBackupEntryCount();
        return ownedEntryCount + backupEntryCount;
    }
}
