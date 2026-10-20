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

package uk.org.hecaton.spi.impl.merge;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractContainerCollectorTest extends HecatonTestSupport {

    private NodeEngineImpl nodeEngine;

    @Before
    public void setUp() {
        HecatonInstance hecatonInstance = createHecatonInstance();
        nodeEngine = getNodeEngineImpl(hecatonInstance);
        warmUpPartitions(hecatonInstance);
    }

    @Test
    public void testAbstractContainerCollector() {
        TestContainerCollector collector = new TestContainerCollector(nodeEngine, true, true);
        assertEqualsStringFormat("Expected the to have %d containers, but found %d", 1, collector.containers.size());

        collector.run();

        assertEqualsStringFormat("Expected %d merging values, but found %d", 1L, collector.getMergingValueCount());
        assertEquals("Expected the collected containers to be removed from the container map", 0, collector.containers.size());
    }

    @Test
    public void testAbstractContainerCollector_withoutContainers() {
        TestContainerCollector collector = new TestContainerCollector(nodeEngine, false, true);
        assertEqualsStringFormat("Expected the to have %d containers, but found %d", 0, collector.containers.size());

        collector.run();

        assertEqualsStringFormat("Expected %d merging values, but found %d", 0L, collector.getMergingValueCount());
        assertEquals("Expected the collected containers to be removed from the container map", 0, collector.containers.size());
    }

    @Test
    public void testAbstractContainerCollector_withoutMergeableContainers() {
        TestContainerCollector collector = new TestContainerCollector(nodeEngine, true, false);
        assertEqualsStringFormat("Expected the to have %d containers, but found %d", 1, collector.containers.size());

        collector.run();

        assertEqualsStringFormat("Expected %d merging values, but found %d", 0L, collector.getMergingValueCount());
        assertEquals("Expected the collected containers to be removed from the container map", 0, collector.containers.size());
    }

    @Test
    public void testEmptyIterator() {
        TestContainerCollector collector = new TestContainerCollector(nodeEngine, false, false);
        Iterator<Object> iterator = collector.containerIterator(0);

        assertInstanceOf(AbstractContainerCollector.EmptyIterator.class, iterator);
        assertFalse("Expected no next elements in iterator", iterator.hasNext());
        try {
            iterator.next();
            fail("Expected EmptyIterator.next() to throw NoSuchElementException");
        } catch (NoSuchElementException expected) {
            ignore(expected);
        }
        try {
            iterator.remove();
            fail("Expected EmptyIterator.remove() to throw UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
            ignore(expected);
        }
    }
}
