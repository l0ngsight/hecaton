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

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.projection.Projection;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.Map.Entry;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapQueryPartitionIteratorTest extends AbstractMapQueryPartitionIteratorTest {

    @Before
    public void init() {
        factory = new TestHecatonFactory();
        instanceProxy = factory.newHecatonInstance(getConfig());
    }

    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Override
    protected <K, V, R> Iterator<R> getIterator(
            IMap<K, V> map,
            int fetchSize,
            int partitionId,
            Projection<Entry<K, V>, R> projection,
            Predicate<K, V> predicate
    ) {
        return ((MapProxyImpl<K, V>) map).iterator(fetchSize, partitionId, projection, predicate);
    }
}
