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
import uk.org.hecaton.config.ListConfig;
import uk.org.hecaton.config.MergePolicyConfig;
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
public class MergePolicyValidatorListIntegrationTest extends AbstractMergePolicyValidatorIntegrationTest {

    @Override
    protected void addConfig(Config config, String name, MergePolicyConfig mergePolicyConfig) {
        ListConfig listConfig = new ListConfig(name)
                .setMergePolicyConfig(mergePolicyConfig);

        config.addListConfig(listConfig);
    }

    @Test
    public void testList_withPutIfAbsentMergePolicy() {
        HecatonInstance hz = getHecatonInstance("putIfAbsent", putIfAbsentMergePolicy);

        hz.getList("putIfAbsent");
    }

    @Test
    public void testList_withHyperLogLogMergePolicy() {
        HecatonInstance hz = getHecatonInstance("cardinalityEstimator", hyperLogLogMergePolicy);

        expectCardinalityEstimatorException(() -> hz.getList("cardinalityEstimator"));
    }

    @Test
    public void testList_withHigherHitsMergePolicy() {
        HecatonInstance hz = getHecatonInstance("higherHits", higherHitsMergePolicy);

        expectedHigherHitsException(() -> hz.getList("higherHits"));
    }

    @Test
    public void testList_withInvalidPolicy() {
        HecatonInstance hz = getHecatonInstance("invalid", invalidMergePolicyConfig);

        expectedInvalidMergePolicyException(() -> hz.getList("invalid"));
    }
}
