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

package uk.org.hecaton.internal.crdt.pncounter;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.crdt.pncounter.PNCounter;
import uk.org.hecaton.internal.monitor.LocalPNCounterStats;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.SplitBrainTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

/**
 * Tests PNCounter statistics are replicated and migrated together with
 * PNCounter state.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PNCounterStatisticsSplitBrainTest extends SplitBrainTestSupport {

    private String counterName = randomMapName("counter-");
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        Config config = super.config();
        config.getPNCounterConfig(counterName).setReplicaCount(2);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HecatonInstance[] instances) {
        waitAllForSafeState(instances);

        getCounter(instances[0]).addAndGet(100);

        assertContainsCounterStatsEventually(true, instances[0], instances[1]);
        assertContainsCounterStatsEventually(false, instances[2]);
    }

    @Override
    protected void onAfterSplitBrainCreated(HecatonInstance[] firstBrain, HecatonInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HecatonInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        getCounter(secondBrain[0]).addAndGet(100);

        assertContainsCounterStatsEventually(true, firstBrain[0], firstBrain[1]);
        assertContainsCounterStatsEventually(true, secondBrain[0]);
    }

    @Override
    protected void onAfterSplitBrainHealed(HecatonInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        assertContainsCounterStatsEventually(true, instances[0], instances[1]);
        assertContainsCounterStatsEventually(false, instances[2]);
    }

    private PNCounter getCounter(HecatonInstance instance) {
        final PNCounter pnCounter = instance.getPNCounter(counterName);
        ((PNCounterProxy) pnCounter).setOperationTryCount(1);
        pnCounter.reset();
        return pnCounter;
    }

    private void assertContainsCounterStatsEventually(final boolean contains, final HecatonInstance... instances) {
        assertTrueEventually(() -> {
            for (HecatonInstance instance : instances) {
                PNCounterService service = getNodeEngineImpl(instance).getService(PNCounterService.SERVICE_NAME);
                Map<String, LocalPNCounterStats> stats = service.getStats();

                assertEquals(contains, stats.containsKey(counterName));
            }
        });
    }
}
