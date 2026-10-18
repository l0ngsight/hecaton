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
import uk.org.hecaton.config.MergePolicyConfig;
import uk.org.hecaton.config.SetConfig;
import uk.org.hecaton.core.HecatonInstance;
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
public class MergePolicyValidatorSetIntegrationTest extends AbstractMergePolicyValidatorIntegrationTest {

    @Override
    protected void addConfig(Config config, String name, MergePolicyConfig mergePolicyConfig) {
        SetConfig setConfig = new SetConfig(name)
                .setMergePolicyConfig(mergePolicyConfig);

        config.addSetConfig(setConfig);
    }

    @Test
    public void testSet_withPutIfAbsentMergePolicy() {
        HecatonInstance hz = getHecatonInstance("putIfAbsent", putIfAbsentMergePolicy);

        hz.getSet("putIfAbsent");
    }

    @Test
    public void testSet_withHyperLogLogMergePolicy() {
        HecatonInstance hz = getHecatonInstance("cardinalityEstimator", hyperLogLogMergePolicy);

        expectCardinalityEstimatorException(() -> hz.getSet("cardinalityEstimator"));
    }

    @Test
    public void testSet_withHigherHitsMergePolicy() {
        HecatonInstance hz = getHecatonInstance("higherHits", higherHitsMergePolicy);

        expectedHigherHitsException(() -> hz.getSet("higherHits"));
    }

    @Test
    public void testSet_withInvalidMergePolicy() {
        HecatonInstance hz = getHecatonInstance("invalid", invalidMergePolicyConfig);

        expectedInvalidMergePolicyException(() -> hz.getSet("invalid"));
    }
}
