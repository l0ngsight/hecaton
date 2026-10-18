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

package uk.org.hecaton.spi.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonRunningInForkJoinPoolTest extends HecatonTestSupport {

    protected static final String MAP_NAME = "near-cached-map";

    protected HecatonInstance hz;

    @Before
    public void setUp() throws Exception {
        Config config = getConfig();

        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setNearCacheConfig(getNearCacheConfig());

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        hz = factory.newHecatonInstance(config);
        hz = factory.newHecatonInstance(config);
    }

    protected static NearCacheConfig getNearCacheConfig() {
        return new NearCacheConfig(MAP_NAME)
                .setInvalidateOnChange(true);
    }

    /**
     * Both Java's parallel-stream-api and Hecaton use
     * {@link ForkJoinPool#commonPool()} for their internal
     * mechanisms to run. We are testing whether or not
     * there is a deadlock during proxy initialization. With
     * near cache enabled, iMap tries to attach listeners at
     * proxy init phase and this triggers deadlock situation.
     */
    @Test
    public void no_deadlock_during_proxy_initialization() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        int count = availableProcessors + 1;
        List<IMap<Object, Object>> maps = IntStream.range(0, count)
                .boxed()
                .parallel()
                // init proxy in parallel
                .map(i -> hz.getMap(MAP_NAME))
                .collect(Collectors.toList());

        assertEquals(count, maps.size());
    }
}
