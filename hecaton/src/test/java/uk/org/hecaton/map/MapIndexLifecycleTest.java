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

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ConfigAccessor;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.ServiceConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.services.CoreService;
import uk.org.hecaton.internal.services.PostJoinAwareService;
import uk.org.hecaton.internal.util.IterationType;
import uk.org.hecaton.jet.impl.JobRepository;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.PartitionContainer;
import uk.org.hecaton.map.impl.query.Query;
import uk.org.hecaton.map.impl.query.QueryResult;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.query.impl.IndexRegistry;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static java.util.Arrays.copyOfRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapIndexLifecycleTest extends HecatonTestSupport {

    private static final int BOOK_COUNT = 1000;

    private final String mapName = randomMapName();

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    boolean globalIndex() {
        return true;
    }

    @Test
    public void recordStoresAndIndexes_createdDestroyedProperly() {
        // GIVEN
        TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory(1);
        HecatonInstance instance1 = createNode(instanceFactory);

        // THEN - initialized
        IMap<Integer, Book> bookMap = instance1.getMap(mapName);
        fillMap(bookMap);
        assertEquals(BOOK_COUNT, bookMap.size());
        assertAllPartitionContainersAreInitialized(instance1);

        // THEN - destroyed
        bookMap.destroy();
        assertAllPartitionContainersAreEmpty(instance1);

        // THEN - initialized
        bookMap = instance1.getMap(mapName);
        fillMap(bookMap);
        assertEquals(BOOK_COUNT, bookMap.size());
        assertAllPartitionContainersAreInitialized(instance1);
    }

    @Test
    public void whenIndexConfigured_existsOnAllMembers() {
        // GIVEN indexes are configured before Hecaton starts
        int clusterSize = 3;
        TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory(clusterSize);
        HecatonInstance[] instances = new HecatonInstance[clusterSize];

        instances[0] = createNode(instanceFactory);
        IMap<Integer, Book> bookMap = instances[0].getMap(mapName);
        fillMap(bookMap);
        assertEquals(BOOK_COUNT, bookMap.size());

        // THEN indexes are migrated and populated on all members
        for (int i = 1; i < clusterSize; i++) {
            instances[i] = createNode(instanceFactory);
            waitAllForSafeState(copyOfRange(instances, 0, i + 1));
            bookMap = instances[i].getMap(mapName);
            assertEquals(BOOK_COUNT, bookMap.keySet().size());
            assertAllPartitionContainersAreInitialized(instances[i]);
            assertGlobalIndexesAreInitialized(instances[i]);
        }
    }

    @Test
    public void whenIndexAddedProgrammatically_existsOnAllMembers() {
        // GIVEN indexes are configured before Hecaton starts
        int clusterSize = 3;
        TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory(clusterSize);
        HecatonInstance[] instances = new HecatonInstance[clusterSize];

        Config config = getConfig().setProperty(ClusterProperty.PARTITION_COUNT.getName(), "4");
        config.getMapConfig(mapName);
        ConfigAccessor.getServicesConfig(config)
                .addServiceConfig(
                        new ServiceConfig()
                                .setName("SlowPostJoinAwareService")
                                .setEnabled(true)
                                .setImplementation(new SlowPostJoinAwareService())
                );

        instances[0] = instanceFactory.newHecatonInstance(config);
        IMap<Integer, Book> bookMap = instances[0].getMap(mapName);
        fillMap(bookMap);
        bookMap.addIndex(getIndexConfig("author", false));
        bookMap.addIndex(getIndexConfig("year", true));
        assertEquals(BOOK_COUNT, bookMap.size());

        // THEN indexes are migrated and populated on all members
        for (int i = 1; i < clusterSize; i++) {
            instances[i] = instanceFactory.newHecatonInstance(config);
            waitAllForSafeState(copyOfRange(instances, 0, i + 1));
            bookMap = instances[i].getMap(mapName);
            assertEquals(BOOK_COUNT, bookMap.keySet().size());
            assertAllPartitionContainersAreInitialized(instances[i]);
            assertGlobalIndexesAreInitialized(instances[i]);
        }
    }

    private void assertGlobalIndexesAreInitialized(HecatonInstance instance) {
        MapServiceContext context = getMapServiceContext(instance);
        final MapContainer mapContainer = context.getMapContainer(mapName);
        if (!globalIndex()) {
            return;
        }
        assertTrueEventually(() -> assertEquals(2, mapContainer.getGlobalIndexRegistry().getIndexes().length));

        assertNotNull("There should be a global index for attribute 'author'",
                mapContainer.getGlobalIndexRegistry().getIndex("author"));
        assertNotNull("There should be a global index for attribute 'year'",
                mapContainer.getGlobalIndexRegistry().getIndex("year"));
        final String authorOwned = findAuthorOwnedBy(instance);
        final Integer yearOwned = findYearOwnedBy(instance);
        assertTrueEventually(() -> assertFalse("Author index should contain records.",
                mapContainer.getGlobalIndexRegistry()
                        .getIndex("author")
                        .getRecords(authorOwned).isEmpty()));

        assertTrueEventually(() -> assertFalse("Year index should contain records",
                mapContainer.getGlobalIndexRegistry().getIndex("year").getRecords(yearOwned).isEmpty()));
    }

    private int numberOfPartitionQueryResults(HecatonInstance instance, int partitionId, String attribute, Comparable value) {
        OperationService operationService = getOperationService(instance);
        Query query = Query.of().mapName(mapName).iterationType(IterationType.KEY).predicate(Predicates.equal(attribute, value))
                .build();
        Operation queryOperation = getMapOperationProvider(instance, mapName).createQueryPartitionOperation(query);
        InternalCompletableFuture<QueryResult> future = operationService
                .invokeOnPartition(MapService.SERVICE_NAME, queryOperation, partitionId);
        return future.join().size();
    }

    private void assertAllPartitionContainersAreEmpty(HecatonInstance instance) {
        MapServiceContext context = getMapServiceContext(instance);
        Map<String, MapContainer> mapContainers = context.getMapContainers();

        // check if there is no map-container exists than INTERNAL_JET_OBJECTS_PREFIX.
        for (MapContainer mapContainer : mapContainers.values()) {
            if (mapContainer.getName().startsWith(JobRepository.INTERNAL_JET_OBJECTS_PREFIX)) {
                continue;
            }

            fail("No such map container must exists: " + mapContainer.getName());
        }

        // check if there is no record-store exists than INTERNAL_JET_OBJECTS_PREFIX.
        int partitionCount = getPartitionCount(instance);
        for (int i = 0; i < partitionCount; i++) {
            PartitionContainer container = context.getPartitionContainer(i);
            Collection<RecordStore> allRecordStores = container.getAllRecordStores();
            for (RecordStore recordStore : allRecordStores) {
                if (recordStore.getName().startsWith(JobRepository.INTERNAL_JET_OBJECTS_PREFIX)) {
                    continue;
                }
                fail("No such record-store must exists: " + recordStore.getName());
            }
        }

        // check if there is no index exist than INTERNAL_JET_OBJECTS_PREFIX.
        for (MapContainer mapContainer : mapContainers.values()) {
            if (mapContainer.getName().startsWith(JobRepository.INTERNAL_JET_OBJECTS_PREFIX)) {
                continue;
            }

            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                assertTrue("indexes not empty", mapContainer.isEmptyIndexRegistry());
            }
        }

    }

    private void assertAllPartitionContainersAreInitialized(HecatonInstance instance) {
        MapServiceContext context = getMapServiceContext(instance);
        int partitionCount = getPartitionCount(instance);
        final AtomicInteger authorRecordsCounter = new AtomicInteger();
        final AtomicInteger yearRecordsCounter = new AtomicInteger();

        String authorOwned = findAuthorOwnedBy(instance);
        Integer yearOwned = findYearOwnedBy(instance);

        for (int i = 0; i < partitionCount; i++) {
            if (!getNode(instance).getPartitionService().isPartitionOwner(i)) {
                continue;
            }

            PartitionContainer container = context.getPartitionContainer(i);

            ConcurrentMap<String, RecordStore> maps = container.getMaps();
            RecordStore recordStore = maps.get(mapName);
            assertNotNull("record store is null: ", recordStore);

            if (!globalIndex()) {
                // also assert contents of partition indexes when NATIVE memory format
                IndexRegistry indexes = context.getExistingMapContainer(mapName)
                        .getPartitionedIndexRegistry().get(i);
                assertNotNull("indexes is null", indexes);
                assertEquals(2, indexes.getIndexes().length);
                assertNotNull("There should be a partition index for attribute 'author'", indexes.getIndex("author"));
                assertNotNull("There should be a partition index for attribute 'year'", indexes.getIndex("year"));

                authorRecordsCounter.getAndAdd(numberOfPartitionQueryResults(instance, i, "author", authorOwned));
                yearRecordsCounter.getAndAdd(numberOfPartitionQueryResults(instance, i, "year", yearOwned));
            }
        }

        if (!globalIndex()) {
            assertTrue("Author index should contain records", authorRecordsCounter.get() > 0);
            assertTrue("Year index should contain records", yearRecordsCounter.get() > 0);
        }
    }

    private int getPartitionCount(HecatonInstance instance) {
        Node node = getNode(instance);
        return node.getProperties().getInteger(ClusterProperty.PARTITION_COUNT);
    }

    private MapServiceContext getMapServiceContext(HecatonInstance instance) {
        NodeEngineImpl nodeEngine1 = getNodeEngineImpl(instance);
        MapService mapService = nodeEngine1.getService(MapService.SERVICE_NAME);
        return mapService.getMapServiceContext();
    }


    private HecatonInstance createNode(TestHecatonInstanceFactory instanceFactory) {
        Config config = getConfig().setProperty(ClusterProperty.PARTITION_COUNT.getName(), "4");
        config.getMapConfig(mapName)
                .addIndexConfig(getIndexConfig("author", false))
                .addIndexConfig(getIndexConfig("year", true))
                .setBackupCount(1);
        return instanceFactory.newHecatonInstance(config);
    }

    public static class Book implements Serializable {

        private long id;
        private String title;
        private String author;
        private int year;

        private Book() {
        }

        Book(long id, String title, String author, int year) {
            this.id = id;
            this.title = title;
            this.author = author;
            this.year = year;
        }

        public long getId() {
            return id;
        }

        public String getTitle() {
            return title;
        }

        public String getAuthor() {
            return author;
        }

        public int getYear() {
            return year;
        }
    }

    private void fillMap(IMap<Integer, Book> map) {
        for (int key = 0; key < BOOK_COUNT; key++) {
            map.put(key, new Book(key, String.valueOf(key), getAuthorNameByKey(key), getYearByKey(key)));
        }
    }

    // A CoreService with a slow post-join op. Its post-join operation will be executed before map's
    // post-join operation, so we can ensure indexes are created via MapReplicationOperation,
    // even though PostJoinMapOperation has not yet been executed.
    public static class SlowPostJoinAwareService implements CoreService, PostJoinAwareService {
        @Override
        public Operation getPostJoinOperation() {
            return new SlowOperation();
        }
    }

    public static class SlowOperation extends Operation {
        @Override
        public void run() {
            sleepSeconds(60);
        }
    }

    private String findAuthorOwnedBy(HecatonInstance hz) {
        int ownedKey = 0;
        Member localMember = hz.getCluster().getLocalMember();
        for (int i = 0; i < BOOK_COUNT; i++) {
            if (localMember.equals(hz.getPartitionService().getPartition(i).getOwner())) {
                ownedKey = i;
                break;
            }
        }
        return getAuthorNameByKey(ownedKey);
    }

    private Integer findYearOwnedBy(HecatonInstance hz) {
        int ownedKey = 0;
        Member localMember = hz.getCluster().getLocalMember();
        for (int i = 0; i < BOOK_COUNT; i++) {
            if (localMember.equals(hz.getPartitionService().getPartition(i).getOwner())) {
                ownedKey = i;
                break;
            }
        }
        return getYearByKey(ownedKey);
    }

    private static int getYearByKey(int key) {
        return 1800 + key % 200;
    }

    private static String getAuthorNameByKey(int key) {
        return String.valueOf(key % 7);
    }

    private static IndexConfig getIndexConfig(String attribute, boolean ordered) {
        return new IndexConfig(ordered ? IndexType.SORTED : IndexType.HASH, attribute).setName(attribute);
    }
}
