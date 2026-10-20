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

package uk.org.hecaton.internal.networking.nio.iobalancer;

import uk.org.hecaton.internal.networking.nio.MigratablePipeline;
import uk.org.hecaton.internal.networking.nio.NioThread;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.internal.util.ItemCounter;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LoadMigrationStrategyTest extends HecatonTestSupport {

    private Map<NioThread, Set<MigratablePipeline>> ownerToPipelines;
    private ItemCounter<MigratablePipeline> loadCounter;
    private LoadImbalance imbalance;

    private LoadMigrationStrategy strategy;

    @Before
    public void setUp() {
        ownerToPipelines = new HashMap<>();
        loadCounter = new ItemCounter<>();
        imbalance = new LoadImbalance(ownerToPipelines, loadCounter);
        strategy = new LoadMigrationStrategy();
    }

    @Test
    public void testImbalanceDetected_shouldReturnFalseWhenNoKnownMinimum() {
        imbalance.minimumLoad = Long.MIN_VALUE;

        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertFalse(imbalanceDetected);
    }

    @Test
    public void testImbalanceDetected_shouldReturnFalseWhenNoKnownMaximum() {
        imbalance.maximumLoad = Long.MAX_VALUE;

        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertFalse(imbalanceDetected);
    }

    @Test
    public void testImbalanceDetected_shouldReturnFalseWhenBalanced() {
        imbalance.maximumLoad = 1000;
        imbalance.minimumLoad = (long) (1000 * 0.8);

        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertFalse(imbalanceDetected);
    }

    @Test
    public void testImbalanceDetected_shouldReturnTrueWhenNotBalanced() {
        imbalance.maximumLoad = 1000;
        imbalance.minimumLoad = (long) (1000 * 0.8) - 1;

        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertTrue(imbalanceDetected);
    }

    @Test
    public void testFindPipelineToMigrate() {
        NioThread srcOwner = mock(NioThread.class);
        NioThread dstOwner = mock(NioThread.class);
        imbalance.srcOwner = srcOwner;
        imbalance.dstOwner = dstOwner;

        imbalance.minimumLoad = 100;
        MigratablePipeline pipeline1 = mock(MigratablePipeline.class);
        loadCounter.set(pipeline1, 100L);
        ownerToPipelines.put(dstOwner, singleton(pipeline1));

        imbalance.maximumLoad = 300;
        MigratablePipeline pipeline2 = mock(MigratablePipeline.class);
        MigratablePipeline pipeline3 = mock(MigratablePipeline.class);
        loadCounter.set(pipeline2, 200L);
        loadCounter.set(pipeline3, 100L);
        ownerToPipelines.put(srcOwner, Set.of(pipeline2, pipeline3));

        MigratablePipeline pipelineToMigrate = strategy.findPipelineToMigrate(imbalance);
        assertEquals(pipeline3, pipelineToMigrate);
    }
}
