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
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.instance.impl.TestUtil.terminateInstance;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalMapStatsTest_WhenMigration extends HecatonTestSupport {

    private HecatonInstance hz1;
    private HecatonInstance hz2;
    private Config config;

    private TestHecatonInstanceFactory factory;

    private IMap<Integer, Integer> map;

    @Before
    public void setUp() {
        config = smallInstanceConfig();
        factory = createHecatonInstanceFactory(2);
        hz1 = factory.newHecatonInstance(config);

        map = hz1.getMap("trial");
    }

    @Test
    public void testHitsGenerated_newNode() {
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.get(i);
        }

        hz2 = factory.newHecatonInstance(config);
        final IMap<Object, Object> trial = hz2.getMap("trial");

        assertTrueEventually(() -> {
            long hits2 = trial.getLocalMapStats().getHits();
            long hits1 = map.getLocalMapStats().getHits();

            assertEquals(100, hits1 + hits2);
        });
    }

    @Test
    public void testHitsGenerated_nodeCrash() {

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.get(i);
        }

        hz2 = factory.newHecatonInstance(config);

        waitAllForSafeState(factory.getAllHecatonInstances());
        terminateInstance(hz2);

        assertTrueEventually(() -> {
            long hits = map.getLocalMapStats().getHits();
            assertEquals(100, hits);
        });
    }
}
