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
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapRemoveAllTest extends HecatonTestSupport {

    private static final int MAP_SIZE = 1000;
    private static final int NODE_COUNT = 3;

    private TestHecatonFactory factory;
    private HecatonInstance client;

    @Before
    public void setUp() {
        factory = new TestHecatonFactory();
        factory.newInstances(getConfig(), NODE_COUNT);

        client = factory.newHecatonClient();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void throws_exception_whenPredicateNull() {
        IMap<Object, Object> map = client.getMap("test");
        assertThatThrownBy(() -> map.removeAll(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("predicate cannot be null");
    }

    @Test
    public void removes_all_entries_whenPredicateTrue() {
        IMap<Integer, Integer> map = client.getMap("test");

        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        map.removeAll(Predicates.alwaysTrue());

        assertEquals(0, map.size());
    }

    @Test
    public void removes_no_entries_whenPredicateFalse() {
        IMap<Integer, Integer> map = client.getMap("test");

        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        map.removeAll(Predicates.alwaysFalse());

        assertEquals(MAP_SIZE, map.size());
    }

    @Test
    public void removes_odd_keys_whenPredicateOdd() {
        IMap<Integer, Integer> map = client.getMap("test");

        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        map.removeAll(new OddFinderPredicate());

        assertEquals(500, map.size());
    }

    private static final class OddFinderPredicate implements Predicate<Integer, Integer> {

        @Override
        public boolean apply(Map.Entry<Integer, Integer> mapEntry) {
            return mapEntry.getKey() % 2 != 0;
        }
    }
}
