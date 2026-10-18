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

import uk.org.hecaton.config.ClasspathXmlConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompositeIndexXmlConfigTest extends HecatonTestSupport {

    IMap<String, Integer> map;

    @Before
    public void before() {
        map = createHecatonInstance().getMap("map");
        for (int i = 0; i < 100; ++i) {
            map.put(Integer.toString(i), i);
        }
    }

    @Override
    protected Config getConfig() {
        return new ClasspathXmlConfig("hecaton-composite-index-xml-config-test.xml");
    }

    @Test
    public void test() {
        Collection<Integer> result = map.values(Predicates.sql("__key = '10' and __key.length = 2"));
        assertEquals(1, result.size());
        assertEquals((Integer) 10, result.iterator().next());
        assertEquals(1, map.getLocalMapStats().getIndexStats().get("testIndex").getQueryCount());
    }

}
