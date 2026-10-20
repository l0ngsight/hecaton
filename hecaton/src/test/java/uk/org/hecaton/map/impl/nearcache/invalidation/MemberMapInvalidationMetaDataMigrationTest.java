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

package uk.org.hecaton.map.impl.nearcache.invalidation;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nearcache.impl.invalidation.Invalidator;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MetaDataGenerator;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.nearcache.MapNearCacheManager;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.config.InMemoryFormat.BINARY;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.internal.util.MapUtil.createHashMap;
import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberMapInvalidationMetaDataMigrationTest extends HecatonTestSupport {

    private static final int MAP_SIZE = 10000;
    private static final String MAP_NAME = "MapInvalidationMetaDataMigrationTest";

    private final TestHecatonInstanceFactory factory = new TestHecatonInstanceFactory();
    private Config config;

    @Before
    public void setUp() {
        config = getConfig(MAP_NAME);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void sequences_migrated_when_newly_joined_nodes_shutdown() {
        HecatonInstance instance1 = factory.newHecatonInstance(config);
        IMap<Object, Object> map = instance1.getMap(MAP_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        assertInvalidationCountEventually(MAP_NAME, MAP_SIZE, instance1);

        Map<Integer, Long> source = getPartitionToSequenceMap(MAP_NAME, instance1);

        HecatonInstance instance2 = factory.newHecatonInstance(config);

        instance1.shutdown();

        HecatonInstance instance3 = factory.newHecatonInstance(config);
        instance2.shutdown();

        waitAllForSafeState(factory.getAllHecatonInstances());

        Map<Integer, Long> destination = getPartitionToSequenceMap(MAP_NAME, instance3);

        assertSequenceNumbersEqual(source, destination);
    }

    @Test
    public void sequences_migrated_when_source_node_shutdown() {
        HecatonInstance instance1 = factory.newHecatonInstance(config);
        IMap<Object, Object> map = instance1.getMap(MAP_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        assertInvalidationCountEventually(MAP_NAME, MAP_SIZE, instance1);

        Map<Integer, Long> source = getPartitionToSequenceMap(MAP_NAME, instance1);

        HecatonInstance instance2 = factory.newHecatonInstance(config);
        HecatonInstance instance3 = factory.newHecatonInstance(config);

        instance1.shutdown();

        waitAllForSafeState(factory.getAllHecatonInstances());

        Map<Integer, Long> destination2 = getPartitionToSequenceMap(MAP_NAME, instance2);
        Map<Integer, Long> destination3 = getPartitionToSequenceMap(MAP_NAME, instance3);

        for (Map.Entry<Integer, Long> entry : destination2.entrySet()) {
            Integer key = entry.getKey();
            Long value = entry.getValue();
            if (value != 0) {
                destination3.put(key, value);
            }
        }

        assertSequenceNumbersEqual(source, destination3);
    }

    @Test
    @Category(SlowTest.class)
    public void sequences_migrated_when_one_node_continuously_starts_and_stops() {
        HecatonInstance instance1 = factory.newHecatonInstance(config);
        IMap<Object, Object> map = instance1.getMap(MAP_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        assertInvalidationCountEventually(MAP_NAME, MAP_SIZE, instance1);

        Map<Integer, Long> source = getPartitionToSequenceMap(MAP_NAME, instance1);

        HecatonInstance instance2 = factory.newHecatonInstance(config);

        final AtomicBoolean stop = new AtomicBoolean();
        Thread shadow = new Thread(() -> {
            while (!stop.get()) {
                HecatonInstance instance = factory.newHecatonInstance(config);
                waitAllForSafeState(instance);
                sleepSeconds(5);
                instance.shutdown();
            }
        });

        shadow.start();
        sleepSeconds(20);
        stop.set(true);
        assertJoinable(shadow);

        instance2.shutdown();

        waitAllForSafeState(factory.getAllHecatonInstances());

        Map<Integer, Long> destination = getPartitionToSequenceMap(MAP_NAME, instance1);

        assertSequenceNumbersEqual(source, destination);
    }

    @Test
    public void uuids_migrated_when_newly_joined_nodes_shutdown() {
        HecatonInstance instance1 = factory.newHecatonInstance(config);
        IMap<Object, Object> map = instance1.getMap(MAP_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        assertInvalidationCountEventually(MAP_NAME, MAP_SIZE, instance1);

        Map<Integer, UUID> source = getPartitionToUuidMap(instance1);

        HecatonInstance instance2 = factory.newHecatonInstance(config);

        instance1.shutdown();

        HecatonInstance instance3 = factory.newHecatonInstance(config);

        instance2.shutdown();

        waitAllForSafeState(factory.getAllHecatonInstances());
        Map<Integer, UUID> destination = getPartitionToUuidMap(instance3);
        assertPartitionUUIDsEqual(source, destination);
    }

    @Test
    public void uuids_migrated_when_source_node_shutdown() {
        final HecatonInstance instance1 = factory.newHecatonInstance(config);
        IMap<Object, Object> map = instance1.getMap(MAP_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        assertInvalidationCountEventually(MAP_NAME, MAP_SIZE, instance1);

        Map<Integer, UUID> source = getPartitionToUuidMap(instance1);

        HecatonInstance instance2 = factory.newHecatonInstance(config);
        HecatonInstance instance3 = factory.newHecatonInstance(config);

        instance1.shutdown();

        waitAllForSafeState(factory.getAllHecatonInstances());

        Map<Integer, UUID> destination2 = getPartitionToUuidMap(instance2);
        Map<Integer, UUID> destination3 = getPartitionToUuidMap(instance3);

        InternalPartitionService partitionService2 = getNodeEngineImpl(instance2).getPartitionService();
        Map<Integer, UUID> merged = mergeOwnedPartitionUuids(partitionService2, destination2, destination3);
        assertPartitionUUIDsEqual(source, merged);
    }

    private void assertInvalidationCountEventually(final String mapName, final int expectedInvalidationCount,
                                                   final HecatonInstance instance) {
        assertTrueEventually(() -> {
            long invalidationCount = calculateNumberOfInvalidationsSoFar(mapName, instance);
            assertEquals(expectedInvalidationCount, invalidationCount);
        });
    }

    protected Config getConfig(String mapName) {
        MapConfig mapConfig = getMapConfig(mapName);

        return getBaseConfig()
                .addMapConfig(mapConfig);
    }

    protected MapConfig getMapConfig(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);

        return new MapConfig(mapName)
                .setNearCacheConfig(nearCacheConfig)
                .setBackupCount(0)
                .setAsyncBackupCount(0);
    }

    protected NearCacheConfig getNearCacheConfig(String mapName) {
        return new NearCacheConfig(mapName)
                .setInMemoryFormat(BINARY)
                .setInvalidateOnChange(true)
                .setCacheLocalEntries(true);
    }

    private static long calculateNumberOfInvalidationsSoFar(String mapName, HecatonInstance instance) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        int partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
        MetaDataGenerator metaDataGenerator = getMetaDataGenerator(nodeEngineImpl);

        long invalidationCount = 0;
        for (int i = 0; i < partitionCount; i++) {
            invalidationCount += metaDataGenerator.currentSequence(mapName, i);
        }
        return invalidationCount;
    }

    private static Map<Integer, Long> getPartitionToSequenceMap(String mapName, HecatonInstance instance) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        int partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
        MetaDataGenerator metaDataGenerator = getMetaDataGenerator(nodeEngineImpl);

        Map<Integer, Long> partitionToSequenceMap = createHashMap(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            partitionToSequenceMap.put(i, metaDataGenerator.currentSequence(mapName, i));
        }
        return partitionToSequenceMap;
    }

    private static Map<Integer, UUID> getPartitionToUuidMap(HecatonInstance instance) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        int partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
        MetaDataGenerator metaDataGenerator = getMetaDataGenerator(nodeEngineImpl);

        Map<Integer, UUID> partitionToUuidMap = createHashMap(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            partitionToUuidMap.put(i, metaDataGenerator.getUuidOrNull(i));
        }
        return partitionToUuidMap;
    }

    private static MetaDataGenerator getMetaDataGenerator(NodeEngineImpl nodeEngineImpl) {
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        return invalidator.getMetaDataGenerator();
    }

    private static Map<Integer, UUID> mergeOwnedPartitionUuids(InternalPartitionService localPartitionService,
                                                               Map<Integer, UUID> localUUIDs, Map<Integer, UUID> remoteUUIDs) {
        int partitionCount = localPartitionService.getPartitionCount();
        Map<Integer, UUID> merged = createHashMap(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            if (localPartitionService.getPartition(i).isLocal()) {
                merged.put(i, localUUIDs.get(i));
            } else {
                merged.put(i, remoteUUIDs.get(i));
            }
        }
        return merged;
    }

    private static void assertSequenceNumbersEqual(Map<Integer, Long> source, Map<Integer, Long> destination) {
        for (Map.Entry<Integer, Long> entry : source.entrySet()) {
            Integer key = entry.getKey();
            Long first = entry.getValue();
            Long last = destination.get(key);

            assertEquals(format(
                    "Expected source and destination sequence numbers to be the same (source: %s) (destination %s)",
                    source, destination),
                    first, last);
        }
    }

    private static void assertPartitionUUIDsEqual(Map<Integer, UUID> source, Map<Integer, UUID> destination) {
        for (Map.Entry<Integer, UUID> entry : source.entrySet()) {
            Integer key = entry.getKey();
            UUID first = entry.getValue();
            UUID last = destination.get(key);

            assertEquals(format(
                    "Expected source and destination partition UUIDs to be the same (source: %s) (destination %s)",
                    source, destination),
                    first, last);
        }
    }
}
