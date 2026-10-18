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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.abs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MonkeyMigrationStrategyTest extends HecatonTestSupport {
    private MigrationStrategy strategy;

    private Map<NioThread, Set<MigratablePipeline>> ownerPipelines;
    private ItemCounter<MigratablePipeline> pipelineLoadCounter;
    private LoadImbalance imbalance;

    @Test
    public void imbalanceDetected_shouldReturnFalseWhenNoPipelineExist() {
        ownerPipelines.put(imbalance.srcOwner, Collections.emptySet());

        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertFalse(imbalanceDetected);
    }

    @Before
    public void setUp() {
        ownerPipelines = new HashMap<>();
        pipelineLoadCounter = new ItemCounter<>();
        imbalance = new LoadImbalance(ownerPipelines, pipelineLoadCounter);
        imbalance.srcOwner = mock(NioThread.class);

        this.strategy = new MonkeyMigrationStrategy();
    }

    @Test
    public void imbalanceDetected_shouldReturnTrueWhenPipelineExist() {
        MigratablePipeline pipeline = mock(MigratablePipeline.class);

        ownerPipelines.put(imbalance.srcOwner, Set.of(pipeline));
        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertTrue(imbalanceDetected);
    }

    @Test
    public void findPipelineToMigrate_shouldWorkEvenWithASinglePipelineAvailable() {
        MigratablePipeline pipeline = mock(MigratablePipeline.class);

        ownerPipelines.put(imbalance.srcOwner, Set.of(pipeline));
        MigratablePipeline pipelineToMigrate = strategy.findPipelineToMigrate(imbalance);
        assertEquals(pipeline, pipelineToMigrate);
    }

    @Test
    public void findPipelineToMigrate_shouldBeFair() {
        int iterationCount = 10000;
        double toleranceFactor = 0.25d;

        MigratablePipeline pipeline1 = mock(MigratablePipeline.class);
        MigratablePipeline pipeline2 = mock(MigratablePipeline.class);
        ownerPipelines.put(imbalance.srcOwner, Set.of(pipeline1, pipeline2));

        assertFairSelection(iterationCount, toleranceFactor, pipeline1, pipeline2);
    }

    private void assertFairSelection(int iterationCount, double toleranceFactor, MigratablePipeline pipeline1, MigratablePipeline pipeline2) {
        int pipeline1Count = 0;
        int pipeline2Count = 0;
        for (int i = 0; i < iterationCount; i++) {
            MigratablePipeline candidate = strategy.findPipelineToMigrate(imbalance);
            if (candidate == pipeline1) {
                pipeline1Count++;
            } else if (candidate == pipeline2) {
                pipeline2Count++;
            } else {
                fail("No pipeline selected");
            }
        }
        int diff = abs(pipeline1Count - pipeline2Count);
        assertTrue(diff < (iterationCount * toleranceFactor));
    }


}
