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

package uk.org.hecaton.map.impl.query;

import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.MigrationEndpoint;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.IterationType;
import uk.org.hecaton.internal.util.SetUtil;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.query.impl.QueryContext;
import uk.org.hecaton.query.impl.QueryableEntry;
import uk.org.hecaton.query.impl.predicates.EqualPredicate;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;
import java.util.UUID;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class})
public class QueryRunnerTest extends HecatonTestSupport {

    private HecatonInstance instance;
    private IMap<String, String> map;
    private QueryRunner queryRunner;
    private MapService mapService;

    private int partitionId;
    private String key;
    private String value;

    @Before
    public void before() {
        instance = createHecatonInstance();
        mapService = getMapService();
        map = instance.getMap(randomName());
        queryRunner = getQueryRunner();

        partitionId = 100;
        key = generateKeyForPartition(instance, partitionId);
        value = randomString();

        map.put(key, value);
    }

    @After
    public void after() {
        if (instance != null) {
            instance.shutdown();
        }
    }

    @Test
    public void assertSequentialQueryRunner() {
        assertEquals(QueryRunner.class, queryRunner.getClass());
    }

    @Test
    public void runFullQuery() {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of()
                .mapName(map.getName())
                .predicate(predicate)
                .iterationType(IterationType.ENTRY)
                .partitionIdSet(SetUtil.allPartitionIds(instance.getPartitionService().getPartitions().size()))
                .build();
        QueryResult result = (QueryResult) queryRunner.runIndexOrPartitionScanQueryOnOwnedPartitions(query);

        assertEquals(1, result.getRows().size());
        assertEquals(map.get(key), toObject(result.getRows().iterator().next().getValue()));
        assertArrayEquals(result.getPartitionIds().toArray(), mapService.getMapServiceContext().getCachedOwnedPartitions().toArray());
    }

    @Test
    public void verifyIndexedQueryFailureWhileMigrating() {
        map.addIndex(IndexType.HASH, "this");
        Predicate predicate = new EqualPredicate("this", value);

        mapService.beforeMigration(new PartitionMigrationEvent(MigrationEndpoint.SOURCE, partitionId, 0, 1, UUID.randomUUID()));

        Query query = Query.of()
                .mapName(map.getName())
                .predicate(predicate)
                .iterationType(IterationType.ENTRY)
                .partitionIdSet(SetUtil.allPartitionIds(instance.getPartitionService().getPartitions().size()))
                .build();
        QueryResult result = (QueryResult) queryRunner.runIndexOrPartitionScanQueryOnOwnedPartitions(query);
        assertNull(result.getPartitionIds());
    }

    @Test
    public void verifyIndexedQueryFailureWhileMigratingInFlight() {
        map.addIndex(IndexType.HASH, "this");

        Predicate predicate = new EqualPredicate("this", value) {
            @Override
            public Set<QueryableEntry> filter(QueryContext queryContext) {
                // start a new migration while executing an indexed query
                mapService.beforeMigration(new PartitionMigrationEvent(MigrationEndpoint.SOURCE, partitionId, 0, 1,
                        UUID.randomUUID()));
                return super.filter(queryContext);
            }
        };
        Query query = Query.of()
                .mapName(map.getName())
                .predicate(predicate)
                .iterationType(IterationType.ENTRY)
                .partitionIdSet(SetUtil.allPartitionIds(instance.getPartitionService().getPartitions().size()))
                .build();
        QueryResult result = (QueryResult) queryRunner.runIndexOrPartitionScanQueryOnOwnedPartitions(query);
        assertNull(result.getPartitionIds());
    }

    @Test
    public void verifyFullScanFailureWhileMigrating() {
        Predicate predicate = new EqualPredicate("this", value);

        mapService.beforeMigration(new PartitionMigrationEvent(MigrationEndpoint.SOURCE, partitionId, 0, 1, UUID.randomUUID()));

        Query query = Query.of()
                .mapName(map.getName())
                .predicate(predicate)
                .iterationType(IterationType.ENTRY)
                .partitionIdSet(SetUtil.allPartitionIds(instance.getPartitionService().getPartitions().size()))
                .build();
        QueryResult result = (QueryResult) queryRunner.runIndexOrPartitionScanQueryOnOwnedPartitions(query);
        assertNull(result.getPartitionIds());
    }

    @Test
    public void verifyFullScanFailureWhileMigratingInFlight() {
        Predicate predicate = new EqualPredicate("this", value) {
            @Override
            protected boolean applyForSingleAttributeValue(Comparable attributeValue) {
                // start a new migration while executing a full scan
                mapService.beforeMigration(new PartitionMigrationEvent(MigrationEndpoint.SOURCE, partitionId, 0, 1,
                        UUID.randomUUID()));
                return super.applyForSingleAttributeValue(attributeValue);
            }
        };
        Query query = Query.of()
                .mapName(map.getName())
                .predicate(predicate)
                .iterationType(IterationType.ENTRY)
                .partitionIdSet(SetUtil.allPartitionIds(instance.getPartitionService().getPartitions().size()))
                .build();
        QueryResult result = (QueryResult) queryRunner.runIndexOrPartitionScanQueryOnOwnedPartitions(query);
        assertNull(result.getPartitionIds());
    }

    private MapService getMapService() {
        return getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
    }

    private QueryRunner getQueryRunner() {
        return mapService.getMapServiceContext().getMapQueryRunner(map.getName());
    }

    private Object toObject(Data data) {
        return getSerializationService(instance).toObject(data);
    }

}
