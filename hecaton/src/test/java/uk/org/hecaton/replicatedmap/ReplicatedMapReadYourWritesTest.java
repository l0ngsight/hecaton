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

package uk.org.hecaton.replicatedmap;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapReadYourWritesTest extends ReplicatedMapAbstractTest {

    @Test
    public void testReadYourWritesBySize() {
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory();
        HecatonInstance instance1 = nodeFactory.newHecatonInstance();
        HecatonInstance instance2 = nodeFactory.newHecatonInstance();
        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        HashMap<Integer, Integer> map = new HashMap<>();
        final int count = 100;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }
        map1.putAll(map);

        assertTrueEventually(() -> {
            assertEquals(count, map1.size());
            assertEquals(count, map2.size());
        });
    }

    @Test
    public void testReadYourWritesByGet() {
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory();
        HecatonInstance instance1 = nodeFactory.newHecatonInstance();
        HecatonInstance instance2 = nodeFactory.newHecatonInstance();
        ReplicatedMap<String, Integer> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, Integer> map2 = instance2.getReplicatedMap("default");
        for (int i = 0; i < 1000; i++) {
            assertEventuallyReadYourWriteByGet(instance2, map1, i);
            assertEventuallyReadYourWriteByGet(instance1, map2, i);
        }
    }

    @Test
    public void testReadYourWritesByContainsKey() {
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory();
        HecatonInstance instance1 = nodeFactory.newHecatonInstance();
        HecatonInstance instance2 = nodeFactory.newHecatonInstance();
        ReplicatedMap<String, Integer> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, Integer> map2 = instance2.getReplicatedMap("default");
        for (int i = 0; i < 1000; i++) {
            assertEventuallyReadYourWriteByContainsKey(instance2, map1, i);
            assertEventuallyReadYourWriteByContainsKey(instance1, map2, i);
        }
    }

    @Test
    public void testReadYourWritesByContainsValue() {
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory();
        HecatonInstance instance1 = nodeFactory.newHecatonInstance();
        HecatonInstance instance2 = nodeFactory.newHecatonInstance();
        ReplicatedMap<String, Integer> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, Integer> map2 = instance2.getReplicatedMap("default");
        for (int i = 0; i < 1000; i++) {
            assertEventuallyReadYourWriteByContainsValue(instance2, map1, i);
            assertEventuallyReadYourWriteByContainsValue(instance1, map2, i);
        }
    }

    private void assertEventuallyReadYourWriteByGet(HecatonInstance instance, ReplicatedMap<String, Integer> map, int value) {
        String key = generateKeyAndPutValue(instance, map, value);
        assertTrueEventually(() -> assertEquals(value, (int) map.get(key)));
    }

    private void assertEventuallyReadYourWriteByContainsKey(HecatonInstance instance, ReplicatedMap<String, Integer> map, int value) {
        String key = generateKeyAndPutValue(instance, map, value);
        assertTrueEventually(() -> assertTrue(map.containsKey(key)));
    }

    private void assertEventuallyReadYourWriteByContainsValue(HecatonInstance instance, ReplicatedMap<String, Integer> map, int value) {
        generateKeyAndPutValue(instance, map, value);
        assertTrueEventually(() -> assertTrue(map.containsValue(value)));
    }

    private String generateKeyAndPutValue(HecatonInstance instance, ReplicatedMap<String, Integer> map, int value) {
        String key = generateKeyOwnedBy(instance);
        map.put(key, value);
        return key;
    }
}
