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
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.IterationType;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.query.Target.TargetMode;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.util.FutureUtil.returnWithDeadline;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class})
public class QueryEngine_DispatchTest extends HecatonTestSupport {

    private HecatonInstance instance;
    private IMap<String, String> map;
    private QueryEngineImpl queryEngine;

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
        queryEngine = new QueryEngineImpl(mapService.getMapServiceContext());

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
    public void dispatchFullQueryOnQueryThread_localMembers() {
        dispatchFullQueryOnQueryThread(TargetMode.LOCAL_NODE);
    }

    @Test
    public void dispatchFullQueryOnQueryThread_allMembers() {
        dispatchFullQueryOnQueryThread(TargetMode.ALL_NODES);
    }

    private void dispatchFullQueryOnQueryThread(TargetMode target) {
        Query query = Query.of().mapName(map.getName()).predicate(Predicates.equal("this", value))
                .partitionIdSet(queryEngine.getAllPartitionIds())
                .iterationType(IterationType.ENTRY).build();
        List<Future<Result>> futures = queryEngine
                .dispatchFullQueryOnQueryThread(query, target);
        Collection<Result> results = returnWithDeadline(futures, 1, TimeUnit.MINUTES);
        QueryResult result = (QueryResult) results.iterator().next();

        assertEquals(1, results.size());
        assertEquals(1, result.size());
        assertEquals(map.get(key), toObject(result.getRows().iterator().next().getValue()));
    }

    @Test
    public void dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread_singlePartition() {
        Query query = Query.of().mapName(map.getName()).predicate(Predicates.equal("this", value))
                .iterationType(IterationType.ENTRY).build();
        Future<Result> future = queryEngine
                .dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(query, partitionId);
        Collection<Result> results = returnWithDeadline(Collections.singletonList(future), 1, TimeUnit.MINUTES);
        QueryResult result = (QueryResult) results.iterator().next();

        assertEquals(1, results.size());
        assertEquals(1, result.size());
        assertEquals(map.get(key), toObject(result.getRows().iterator().next().getValue()));
    }

    private Object toObject(Data data) {
        return getSerializationService(instance).toObject(data);
    }

}
