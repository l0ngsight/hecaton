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

package uk.org.hecaton.multimap;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MultiMapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiMapReturnedCollectionTest extends HecatonTestSupport {

    @Test
    public void testGet_withSetBackedValueCollection() {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.SET);

        multiMap.put(0, 1);
        multiMap.put(0, 2);
        multiMap.put(0, 3);

        Collection<Integer> collection = multiMap.get(0);

        assertTrue(collection instanceof Set);
    }

    @Test
    public void testGet_withSetBackedValueCollection_onEmptyMultiMap() {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.SET);
        Collection<Integer> collection = multiMap.get(0);

        assertTrue(collection instanceof Set);
    }

    @Test
    public void testGet_withListBackedValueCollection() {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.LIST);

        multiMap.put(0, 1);
        multiMap.put(0, 2);
        multiMap.put(0, 3);

        Collection<Integer> collection = multiMap.get(0);

        assertTrue(collection instanceof List);
    }

    @Test
    public void testGet_withListBackedValueCollection_onEmptyMultiMap() {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.LIST);
        Collection<Integer> collection = multiMap.get(0);

        assertTrue(collection instanceof List);
    }

    @Test
    public void testRemove_withSetBackedValueCollection() {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.SET);

        multiMap.put(0, 1);
        multiMap.put(0, 2);
        multiMap.put(0, 3);

        Collection<Integer> collection = multiMap.remove(0);

        assertTrue(collection instanceof Set);
    }

    @Test
    public void testRemove_withSetBackedValueCollection_onEmptyMultiMap() {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.SET);
        Collection<Integer> collection = multiMap.remove(0);

        assertTrue(collection instanceof Set);
    }

    @Test
    public void testRemove_withListBackedValueCollection() {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.LIST);

        multiMap.put(0, 1);
        multiMap.put(0, 2);
        multiMap.put(0, 3);

        Collection<Integer> collection = multiMap.remove(0);

        assertTrue(collection instanceof List);
    }

    @Test
    public void testRemove_withListBackedValueCollection_onEmptyMultiMap() {
        MultiMap<Integer, Integer> multiMap = createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType.LIST);
        Collection<Integer> collection = multiMap.remove(0);

        assertTrue(collection instanceof List);
    }

    private MultiMap<Integer, Integer> createMultiMapWithCollectionType(MultiMapConfig.ValueCollectionType collectionType) {
        String multiMapName = randomMapName();
        Config config = new Config();
        config.getMultiMapConfig(multiMapName)
                .setValueCollectionType(collectionType);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance[] instances = factory.newInstances(config);
        return instances[0].getMultiMap(multiMapName);
    }
}
