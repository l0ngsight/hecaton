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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.collection.PartitionIdSet;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static uk.org.hecaton.internal.util.IterationType.ENTRY;
import static uk.org.hecaton.internal.util.IterationType.KEY;
import static uk.org.hecaton.internal.util.IterationType.VALUE;
import static uk.org.hecaton.map.impl.query.Target.createPartitionTarget;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class})
public class QueryEngineImplTest extends HecatonTestSupport {

    private HecatonInstance instance;
    private IMap<String, String> map;
    private QueryEngine queryEngine;

    private int partitionId;
    private String key;
    private String value;

    @Before
    public void before() {
        Config config = regularInstanceConfig()
                .setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        instance = createHecatonInstance(config);
        map = instance.getMap(randomName());
        MapService mapService = getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
        queryEngine = mapService.getMapServiceContext().getQueryEngine(map.getName());

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
    public void runQueryOnAllPartitions() {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of().mapName(map.getName()).predicate(predicate).iterationType(KEY).build();

        QueryResult result = queryEngine.execute(query, Target.ALL_NODES);

        assertEquals(1, result.size());
        assertEquals(key, toObject(result.iterator().next().getKey()));
    }

    @Test
    public void runQueryOnLocalPartitions() {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of().mapName(map.getName()).predicate(predicate).iterationType(KEY).build();

        QueryResult result = queryEngine.execute(query, Target.LOCAL_NODE);

        assertEquals(1, result.size());
        assertEquals(key, toObject(result.iterator().next().getKey()));
    }

    @Test
    public void runQueryOnAllPartitions_key() {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of().mapName(map.getName()).predicate(predicate).iterationType(KEY).build();

        QueryResult result = queryEngine.execute(query, Target.ALL_NODES);

        assertEquals(1, result.size());
        assertEquals(key, toObject(result.iterator().next().getKey()));
    }

    @Test
    public void runQueryOnAllPartitions_value() {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of().mapName(map.getName()).predicate(predicate).iterationType(VALUE).build();

        QueryResult result = queryEngine.execute(query, Target.ALL_NODES);

        assertEquals(1, result.size());
        assertEquals(value, toObject(result.iterator().next().getValue()));
    }

    @Test
    public void runQueryOnGivenPartition() {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of().mapName(map.getName()).predicate(predicate).iterationType(ENTRY).build();

        QueryResult result = queryEngine.execute(query, createPartitionTarget(new PartitionIdSet(271, partitionId)));

        assertEquals(1, result.size());
        assertEquals(key, toObject(((Map.Entry) result.iterator().next()).getKey()));
        assertEquals(map.get(key), toObject(((Map.Entry) result.iterator().next()).getValue()));
    }

    private Object toObject(Object data) {
        return getSerializationService(instance).toObject(data);
    }

}
