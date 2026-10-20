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

package uk.org.hecaton.internal.config;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MergePolicyConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.config.mergepolicies.ComplexCustomMergePolicy;
import uk.org.hecaton.internal.config.mergepolicies.CustomMapMergePolicy;
import uk.org.hecaton.spi.merge.MergingCosts;
import uk.org.hecaton.spi.merge.MergingExpirationTime;
import uk.org.hecaton.spi.merge.MergingLastStoredTime;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests the integration of the {@link MergePolicyValidator}
 * into the proxy creation of split-brain capable data structures.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MergePolicyValidatorMapIntegrationTest extends AbstractMergePolicyValidatorIntegrationTest {

    private boolean perEntryStatsEnabled = true;

    @Override
    protected void addConfig(Config config, String name, MergePolicyConfig mergePolicyConfig) {
        MapConfig mapConfig = new MapConfig(name)
                .setPerEntryStatsEnabled(perEntryStatsEnabled)
                .setMergePolicyConfig(mergePolicyConfig);

        config.addMapConfig(mapConfig);
    }

    @Test
    public void testMap_withPutIfAbsentMergePolicy() {
        HecatonInstance hz = getHecatonInstance("putIfAbsent", putIfAbsentMergePolicy);

        hz.getMap("putIfAbsent");
    }

    @Test
    public void testMap_withHyperLogLogMergePolicy() {
        expectCardinalityEstimatorException(() -> {
            HecatonInstance hz = getHecatonInstance("cardinalityEstimator", hyperLogLogMergePolicy);

            hz.getMap("cardinalityEstimator");
        });
    }

    @Test
    public void testMap_withHigherHitsMergePolicy() {
        perEntryStatsEnabled = true;
        HecatonInstance hz = getHecatonInstance("higherHits", higherHitsMergePolicy);

        hz.getMap("higherHits");
    }

    @Test
    public void testMap_withInvalidMergePolicy() {
        expectedInvalidMergePolicyException(() -> {
            HecatonInstance hz = getHecatonInstance("invalid", invalidMergePolicyConfig);

            hz.getMap("invalid");
        });
    }

    /**
     * The {@link MergingExpirationTime} is just provided if map statistics are enabled.
     */
    @Test
    public void testMap_withExpirationTimeMergePolicy() {
        HecatonInstance hz = getHecatonInstance("expirationTime", expirationTimeMergePolicy);

        hz.getMap("expirationTime");
    }

    @Test
    public void testMap_withExpirationTimeMergePolicy_withStatsEnabled() {
        perEntryStatsEnabled = true;
        HecatonInstance hz = getHecatonInstance("expirationTime", expirationTimeMergePolicy);

        hz.getMap("expirationTime");
    }

    /**
     * The {@link MergingLastStoredTime} is just provided if map statistics are enabled.
     */
    @Test
    public void testMap_withLastStoredTimeMergePolicy() {
        perEntryStatsEnabled = false;
        expectedMapStatisticsDisabledException(lastStoredTimeMergePolicy, () -> {
            HecatonInstance hz = getHecatonInstance("lastStoredTime", lastStoredTimeMergePolicy);

            hz.getMap("lastStoredTime");
        });

    }

    @Test
    public void testMap_withLastStoredMergePolicy_withStatsEnabled() {
        perEntryStatsEnabled = true;
        HecatonInstance hz = getHecatonInstance("lastStoredTime", lastStoredTimeMergePolicy);

        hz.getMap("lastStoredTime");
    }

    /**
     * The {@link MergingLastStoredTime} is just provided if map statistics are enabled.
     */
    @Test
    public void testMap_withLastStoredTimeMergePolicyNoTypeVariable() {
        perEntryStatsEnabled = false;
        expectedMapStatisticsDisabledException(lastStoredTimeMergePolicyNoTypeVariable, () -> {
            HecatonInstance hz = getHecatonInstance("lastStoredTimeNoTypeVariable", lastStoredTimeMergePolicyNoTypeVariable);

            hz.getMap("lastStoredTimeNoTypeVariable");
        });

    }

    @Test
    public void testMap_withLastStoredMergePolicyNoTypeVariable_withStatsEnabled() {
        perEntryStatsEnabled = true;
        HecatonInstance hz = getHecatonInstance("lastStoredTimeNoTypeVariable", lastStoredTimeMergePolicyNoTypeVariable);

        hz.getMap("lastStoredTimeNoTypeVariable");
    }

    /**
     * IMap always provides the required {@link MergingCosts} from the {@link ComplexCustomMergePolicy},
     * but the required {@link MergingExpirationTime} are just provided if map statistics are enabled.
     */
    @Test
    public void testMap_withComplexCustomMergePolicy() {
        HecatonInstance hz = getHecatonInstance("complexCustom", complexCustomMergePolicy);

        hz.getMap("complexCustom");
    }

    @Test
    public void testMap_withComplexCustomMergePolicy_withStatsEnabled() {
        perEntryStatsEnabled = true;
        HecatonInstance hz = getHecatonInstance("complexCustom", complexCustomMergePolicy);

        hz.getMap("complexCustom");
    }

    /**
     * The required {@link MergingExpirationTime} and {@link MergingLastStoredTime} from the {@link CustomMapMergePolicy}
     * are just provided if map statistics are enabled.
     */
    @Test
    public void testMap_withCustomMapMergePolicy() {
        perEntryStatsEnabled = false;
        expectedMapStatisticsDisabledException(customMapMergePolicy, () -> {
            HecatonInstance hz = getHecatonInstance("customMap", customMapMergePolicy);

            hz.getMap("customMap");
        });
    }

    @Test
    public void testMap_withCustomMapMergePolicy_withStatsEnabled() {
        perEntryStatsEnabled = true;
        HecatonInstance hz = getHecatonInstance("customMap", customMapMergePolicy);

        hz.getMap("customMap");
    }

    /**
     * The required {@link MergingExpirationTime} and {@link MergingLastStoredTime} from the {@link CustomMapMergePolicy}
     * are just provided if map statistics are enabled.
     */
    @Test
    public void testMap_withCustomMapMergePolicyNoTypeVariable() {
        perEntryStatsEnabled = false;
        expectedMapStatisticsDisabledException(customMapMergePolicyNoTypeVariable, () -> {
            HecatonInstance hz = getHecatonInstance("customMapNoTypeVariable", customMapMergePolicyNoTypeVariable);

            hz.getMap("customMapNoTypeVariable");
        });

    }

    @Test
    public void testMap_withCustomMapMergePolicyNoTypeVariable_withStatsEnabled() {
        perEntryStatsEnabled = true;
        HecatonInstance hz = getHecatonInstance("customMapNoTypeVariable", customMapMergePolicyNoTypeVariable);

        hz.getMap("customMapNoTypeVariable");
    }
}
