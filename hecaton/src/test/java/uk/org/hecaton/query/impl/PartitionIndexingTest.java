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

package uk.org.hecaton.query.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ConfigAccessor;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.ServiceConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.MigrationAwareService;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.partition.PartitionReplicationEvent;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static uk.org.hecaton.test.Accessors.getAllIndexes;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionIndexingTest extends HecatonTestSupport {

    private static final int ENTRIES = 10000;
    private static final int ASSERT_TRUE_EVENTUALLY_TIMEOUT = 10;
    protected static final String MAP_NAME = "map";

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    protected TestHecatonInstanceFactory factory;

    private MigrationFailingService migrationFailingService = new MigrationFailingService();

    protected TestHecatonInstanceFactory createFactory() {
        return createHecatonInstanceFactory();
    }

    @Before
    public void before() {
        factory = createFactory();
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "101");
        config.getMapConfig(MAP_NAME).setInMemoryFormat(inMemoryFormat);
        ServiceConfig serviceConfig = new ServiceConfig().setEnabled(true)
                .setImplementation(migrationFailingService)
                .setName(MigrationFailingService.class.getName());
        ConfigAccessor.getServicesConfig(config).addServiceConfig(serviceConfig);
        return config;
    }

    protected IMap<Integer, Integer> createClientFor(IMap<Integer, Integer> map) {
        return map;
    }

    @Test
    public void testOnPreConfiguredIndexes() {
        Supplier<Config> configSupplier = () -> {
            Config config = getConfig();
            config.getMapConfig(MAP_NAME).addIndexConfig(new IndexConfig(IndexType.HASH, "this"));
            config.getMapConfig(MAP_NAME).addIndexConfig(new IndexConfig(IndexType.SORTED, "__key"));
            return config;
        };

        HecatonInstance instance1 = factory.newHecatonInstance(configSupplier.get());
        int expectedPartitions = getPartitionService(instance1).getPartitionCount();

        IMap<Integer, Integer> map1 = instance1.getMap(MAP_NAME);
        assertPartitionsIndexedCorrectlyEventually(expectedPartitions, map1);

        IMap<Integer, Integer> client1 = createClientFor(map1);
        for (int i = 0; i < ENTRIES; ++i) {
            client1.put(i, i);
        }
        assertPartitionsIndexedCorrectlyEventually(expectedPartitions, map1);

        HecatonInstance instance2 = factory.newHecatonInstance(configSupplier.get());
        IMap<Integer, Integer> map2 = instance2.getMap(MAP_NAME);
        waitAllForSafeState(instance1, instance2);
        assertPartitionsIndexedCorrectlyEventually(expectedPartitions, map1, map2);

        HecatonInstance instance3 = factory.newHecatonInstance(configSupplier.get());
        IMap<Integer, Integer> map3 = instance3.getMap(MAP_NAME);
        waitAllForSafeState(instance1, instance2, instance3);
        assertPartitionsIndexedCorrectlyEventually(expectedPartitions, map1, map2, map3);

        instance2.shutdown();
        waitAllForSafeState(instance1, instance3);
        assertPartitionsIndexedCorrectlyEventually(expectedPartitions, map1, map3);

        migrationFailingService.fail = true;
        HecatonInstance instance4 = factory.newHecatonInstance(configSupplier.get());
        IMap<Integer, Integer> map4 = instance4.getMap(MAP_NAME);
        waitAllForSafeState(instance1, instance3, instance4);
        assertPartitionsIndexedCorrectlyEventually(expectedPartitions, map1, map3, map4);
        assertTrue(migrationFailingService.rolledBack);
    }

    @Test
    public void testOnProgrammaticallyAddedIndexes() {
        HecatonInstance instance1 = factory.newHecatonInstance(getConfig());
        int expectedPartitions = getPartitionService(instance1).getPartitionCount();

        IMap<Integer, Integer> map1 = instance1.getMap(MAP_NAME);
        assertPartitionsIndexedCorrectlyEventually(expectedPartitions, map1);

        IMap<Integer, Integer> client1 = createClientFor(map1);
        for (int i = 0; i < ENTRIES; ++i) {
            client1.put(i, i);
        }
        client1.addIndex(IndexType.HASH, "this");
        assertPartitionsIndexedCorrectlyEventually(expectedPartitions, map1);

        HecatonInstance instance2 = factory.newHecatonInstance(getConfig());
        IMap<Integer, Integer> map2 = instance2.getMap(MAP_NAME);
        waitAllForSafeState(instance1, instance2);
        assertPartitionsIndexedCorrectlyEventually(expectedPartitions, map1, map2);

        HecatonInstance instance3 = factory.newHecatonInstance(getConfig());
        IMap<Integer, Integer> map3 = instance3.getMap(MAP_NAME);
        waitAllForSafeState(instance1, instance2, instance3);
        assertPartitionsIndexedCorrectlyEventually(expectedPartitions, map1, map2, map3);

        instance2.shutdown();
        waitAllForSafeState(instance1, instance3);
        assertPartitionsIndexedCorrectlyEventually(expectedPartitions, map1, map3);

        IMap<Integer, Integer> client3 = createClientFor(map3);
        client3.addIndex(IndexType.HASH, "__key");
        assertPartitionsIndexedCorrectlyEventually(expectedPartitions, map1, map3);

        migrationFailingService.fail = true;
        HecatonInstance instance4 = factory.newHecatonInstance(getConfig());
        IMap<Integer, Integer> map4 = instance4.getMap(MAP_NAME);
        waitAllForSafeState(instance1, instance3, instance4);
        assertPartitionsIndexedCorrectlyEventually(expectedPartitions, map1, map3, map4);
        assertTrue(migrationFailingService.rolledBack);
    }

    /**
     * We assert eventually because waiting for safe state does not mean all partition migration events are received
     * and processed. We should give MapMigrationAwareService some time to process all partition migration events.
     */
    private static void assertPartitionsIndexedCorrectlyEventually(int expectedPartitions, IMap... maps) {
        assertTrueEventually(() -> assertPartitionsIndexedCorrectly(expectedPartitions, maps), ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    private static void assertPartitionsIndexedCorrectly(int expectedPartitions, IMap... maps) {
        Map<String, BitSet> indexToPartitions = new HashMap<>();

        for (IMap map : maps) {
            for (IndexRegistry indexes : getAllIndexes(map)) {
                for (InternalIndex index : indexes.getIndexes()) {
                    String indexName = index.getName();
                    BitSet indexPartitions = indexToPartitions.get(indexName);
                    if (indexPartitions == null) {
                        indexPartitions = new BitSet();
                        indexToPartitions.put(indexName, indexPartitions);
                    }

                    for (int partition = 0; partition < expectedPartitions; ++partition) {
                        if (index.hasPartitionIndexed(partition)) {
                            assertFalse("partition #" + partition + " is already indexed by " + indexName,
                                    indexPartitions.get(partition));
                            indexPartitions.set(partition);
                        }
                    }
                }
            }
        }

        for (Map.Entry<String, BitSet> indexEntry : indexToPartitions.entrySet()) {
            String indexName = indexEntry.getKey();
            BitSet indexPartitions = indexEntry.getValue();

            int actualPartitions = indexPartitions.cardinality();
            assertEquals(indexName + " is missing " + (expectedPartitions - actualPartitions) + " partitions", expectedPartitions,
                    actualPartitions);
        }
    }

    private static class MigrationFailingService implements MigrationAwareService {
        public volatile boolean fail = false;
        public volatile boolean rolledBack = false;

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
            return null;
        }

        @Override
        public void beforeMigration(PartitionMigrationEvent event) {
            if (fail && !rolledBack) {
                throw new RuntimeException("migration intentionally failed");
            }
        }

        @Override
        public void commitMigration(PartitionMigrationEvent event) {
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
            rolledBack = true;
        }
    }

}
