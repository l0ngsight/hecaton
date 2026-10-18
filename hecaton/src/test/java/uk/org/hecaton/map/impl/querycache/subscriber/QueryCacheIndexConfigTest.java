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

package uk.org.hecaton.map.impl.querycache.subscriber;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.PredicateConfig;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.query.impl.IndexUtils;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheIndexConfigTest extends HecatonTestSupport {

    @Test
    public void testIndexConfigIsRespected() {
        IndexConfig indexConfig = IndexUtils.createTestIndexConfig(IndexType.SORTED, "field");

        final Config config = new Config();
        final MapConfig mapConfig = new MapConfig("map").addQueryCacheConfig(
                new QueryCacheConfig().setName("query-cache")
                                      .setPredicateConfig(new PredicateConfig(Predicates.alwaysTrue()))
                                      .addIndexConfig(indexConfig));
        config.addMapConfig(mapConfig);

        final HecatonInstance instance = createHecatonInstance(config);
        final IMap<Object, Object> map = instance.getMap("map");
        final DefaultQueryCache<Object, Object> cache = (DefaultQueryCache<Object, Object>) map.getQueryCache("query-cache");

        assertNotNull(cache.indexRegistry.getIndex(indexConfig.getName()));
        assertTrue(cache.indexRegistry.getIndex(indexConfig.getName()).isOrdered());
    }

}
