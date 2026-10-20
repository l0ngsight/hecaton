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

package uk.org.hecaton.client.map;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.LocalMapStatsTest;
import uk.org.hecaton.map.LocalMapStats;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapStatsTest extends LocalMapStatsTest {
    private final TestHecatonFactory factory = new TestHecatonFactory();

    private HecatonInstance client;
    private HecatonInstance member;

    @Before
    @Override
    public void setUp() {
        member = factory.newHecatonInstance(createMemberConfig());
        client = factory.newHecatonClient();
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Override
    protected LocalMapStats getMapStats(String mapName) {
        return member.getMap(mapName).getLocalMapStats();
    }

    @Override
    public void testOtherOperationCount_localKeySet() {
        // localKeySet is not supported on client
    }

    @Override
    protected <K, V> IMap<K, V> getMap(String mapName) {
        return client.getMap(mapName);
    }
}
