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
import uk.org.hecaton.map.MapLiteMemberTest;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapQueryLiteMemberTest extends HecatonTestSupport {

    private TestHecatonFactory factory;

    private IMap<Integer, Object> map;

    @Before
    public void setUp() {
        factory = new TestHecatonFactory();
        factory.newHecatonInstance(getConfig());
        factory.newHecatonInstance(getConfig().setLiteMember(true));
        HecatonInstance client = factory.newHecatonClient();
        map = client.getMap(randomMapName());
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testMapValuesQuery() {
        MapLiteMemberTest.testMapValuesQuery(map);
    }

    @Test
    public void testMapKeysQuery() {
        MapLiteMemberTest.testMapKeysQuery(map);
    }
}
