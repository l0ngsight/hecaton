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

package uk.org.hecaton.map.impl.operation;

import uk.org.hecaton.map.impl.MapEntries;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PutAllPartitionAwareOperationFactoryTest extends HecatonTestSupport {

    private PutAllPartitionAwareOperationFactory factory;

    @Before
    public void setUp() throws Exception {
        String name = randomMapName();
        int[] partitions = new int[0];
        MapEntries[] mapEntries = new MapEntries[0];
        factory = getFactory(name, partitions, mapEntries);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateOperation() {
        factory.createOperation();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatePartitionOperation() {
        factory.createPartitionOperation(0);
    }

    protected PutAllPartitionAwareOperationFactory getFactory(String name, int[] partitions, MapEntries[] mapEntries) {
        return new PutAllPartitionAwareOperationFactory(name, partitions, mapEntries, true);
    }
}
