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
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryResultSizeExceededException;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.internal.util.IterationType.ENTRY;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class QueryEngineImpl_queryLocalPartition_resultSizeLimitTest extends HecatonTestSupport {

    private static final int RESULT_SIZE_LIMIT = QueryResultSizeLimiter.MINIMUM_MAX_RESULT_LIMIT + 2342;
    private static final int PARTITION_COUNT = 2;
    private static final int PARTITION_ID = 0;

    private HecatonInstance hz;
    private IMap<String, String> map;
    private QueryEngineImpl queryEngine;
    private int limit;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        // we reduce the number of partitions to speed up content generation
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
        config.setProperty(ClusterProperty.QUERY_RESULT_SIZE_LIMIT.getName(), String.valueOf(RESULT_SIZE_LIMIT));

        hz = createHecatonInstance(config);
        map = hz.getMap(randomName());

        MapService mapService = getNodeEngineImpl(hz).getService(MapService.SERVICE_NAME);
        queryEngine = new QueryEngineImpl(mapService.getMapServiceContext());

        // we just fill a single partition, so we get the NodeResultLimit for a single partition as well
        QueryResultSizeLimiter resultSizeLimiter = queryEngine.getQueryResultSizeLimiter();
        limit = (int) resultSizeLimiter.getNodeResultLimit(1);
    }

    @Test
    public void checkResultSize_limitNotExceeded() {
        fillPartition(limit - 1);

        Query query = Query.of().mapName(map.getName()).predicate(Predicates.alwaysTrue()).iterationType(ENTRY).build();
        QueryResult result = (QueryResult) queryEngine.execute(query, Target.LOCAL_NODE);

        assertEquals(limit - 1, result.size());
    }

    @Test
    public void checkResultSize_limitNotEquals() {
        fillPartition(limit);

        Query query = Query.of().mapName(map.getName()).predicate(Predicates.alwaysTrue()).iterationType(ENTRY).build();
        QueryResult result = (QueryResult) queryEngine.execute(query, Target.LOCAL_NODE);

        assertEquals(limit, result.size());
    }

    @Ignore
    @Test(expected = QueryResultSizeExceededException.class)
    public void checkResultSize_limitExceeded() {
        fillPartition(limit + 1);

        Query query = Query.of().mapName(map.getName()).predicate(Predicates.alwaysTrue()).iterationType(ENTRY).build();
        queryEngine.execute(query, Target.LOCAL_NODE);
    }

    private void fillPartition(int count) {
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(hz, PARTITION_ID);
            map.put(key, "");
        }
    }
}
