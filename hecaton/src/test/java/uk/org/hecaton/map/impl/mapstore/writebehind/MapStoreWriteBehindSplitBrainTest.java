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

package uk.org.hecaton.map.impl.mapstore.writebehind;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.config.MergePolicyConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.merge.DiscardMergePolicy;
import uk.org.hecaton.spi.merge.HigherHitsMergePolicy;
import uk.org.hecaton.spi.merge.LatestAccessMergePolicy;
import uk.org.hecaton.spi.merge.LatestUpdateMergePolicy;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.spi.merge.PutIfAbsentMergePolicy;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.SplitBrainTestSupport;
import uk.org.hecaton.test.annotation.SlowTest;

import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import javax.annotation.Nonnull;

import static uk.org.hecaton.config.InMemoryFormat.BINARY;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;


@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category(SlowTest.class)
public class MapStoreWriteBehindSplitBrainTest extends SplitBrainTestSupport {

    private static final int[] BRAINS = new int[]{3, 3};

    private static final int NUMBER_OF_ITEMS_TO_WRITE = 2000;

    @Parameters(name = "format:{0}, mergePolicy:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY, DiscardMergePolicy.class.getName()},
                {BINARY, HigherHitsMergePolicy.class.getName()},
                {BINARY, LatestAccessMergePolicy.class.getName()},
                {BINARY, LatestUpdateMergePolicy.class.getName()},
                {BINARY, PassThroughMergePolicy.class.getName()},
                {BINARY, PutIfAbsentMergePolicy.class.getName()},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public String mergePolicyClassName;

    @SuppressWarnings("null")
    protected @Nonnull String mapName = randomMapName("map-");

    private @Nonnull TrackingMapStore<Integer, Integer> mapStore = new TrackingMapStore<Integer, Integer>();

    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected int[] brains() {
        return BRAINS;
    }

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClassName)
                .setBatchSize(10);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig
                .setImplementation(mapStore)
                .setWriteDelaySeconds(1)
                .setWriteBatchSize(10)
                .setWriteCoalescing(false);

        Config config = super.config();
        config.getMapConfig(mapName)
                .setInMemoryFormat(inMemoryFormat)
                .setMergePolicyConfig(mergePolicyConfig)
                .setBackupCount(1)
                .setMapStoreConfig(mapStoreConfig)
                .setStatisticsEnabled(true)
                .setPerEntryStatsEnabled(true);

        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HecatonInstance[] instances) throws InterruptedException {
        writeToMapAndAssertWriteBehind(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HecatonInstance[] firstBrain, HecatonInstance[] secondBrain) {
        @Nonnull MergeLifecycleListener mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HecatonInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }
        this.mergeLifecycleListener = mergeLifecycleListener;
    }

    @Override
    protected void onAfterSplitBrainHealed(HecatonInstance[] instances) throws InterruptedException {
        // wait until merge completes
        mergeLifecycleListener.await();

        // wait until any flushing of maps following merge is complete
        assertTrueEventually("write behinds complete", () ->
            assertTrue(mapStore.lastWriteAtLeastMsAgo(2000)), 30);

        writeToMapAndAssertWriteBehind(instances);
    }

    private void writeToMapAndAssertWriteBehind(HecatonInstance[] instances) throws InterruptedException {
        IMap<Integer, Integer> map = instances[0].getMap(mapName);
        for (int i = 0; i < NUMBER_OF_ITEMS_TO_WRITE; i++) {
            mapStore.expectWrite(i, i);
            map.put(i, i);
            Thread.sleep(3); // ensure writes happen across multiple write-behind invocations
        }
        map.flush();

        assertTrueEventually("expected all entries to be written", () ->
            assertTrue(mapStore.allExpectedWritesComplete()), 30);
    }
}
