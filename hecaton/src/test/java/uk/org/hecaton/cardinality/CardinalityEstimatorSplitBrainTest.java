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

package uk.org.hecaton.cardinality;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MergePolicyConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.merge.DiscardMergePolicy;
import uk.org.hecaton.spi.merge.HyperLogLogMergePolicy;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.spi.merge.PutIfAbsentMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
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

import static uk.org.hecaton.cardinality.CardinalityEstimatorTestUtil.getBackupEstimate;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category(SlowTest.class)
public class CardinalityEstimatorSplitBrainTest extends SplitBrainTestSupport {

    private final int INITIAL_COUNT = 100000;
    private final int EXTRA_COUNT = 10000;
    private final int TOTAL_COUNT = INITIAL_COUNT + (brains().length * EXTRA_COUNT);

    private final String estimatorNameA = randomMapName("estimatorA-");
    private final String estimatorNameB = randomMapName("estimatorB-");
    private CardinalityEstimator estimatorA1;
    private CardinalityEstimator estimatorA2;
    private CardinalityEstimator estimatorB1;
    private CardinalityEstimator estimatorB2;
    private long expectedEstimateA;
    private long expectedEstimateB;
    private long backupEstimateA;
    private long backupEstimateB;
    private MergeLifecycleListener mergeLifecycleListener;

    @Parameters(name = "mergePolicy:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{
                DiscardMergePolicy.class,
                HyperLogLogMergePolicy.class,
                PassThroughMergePolicy.class,
                PutIfAbsentMergePolicy.class,
        });
    }

    @Parameter
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getCardinalityEstimatorConfig(estimatorNameA)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setMergePolicyConfig(mergePolicyConfig);
        config.getCardinalityEstimatorConfig(estimatorNameB)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setMergePolicyConfig(mergePolicyConfig);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HecatonInstance[] instances) {
        CardinalityEstimator estimator = instances[0].getCardinalityEstimator(estimatorNameA);
        for (int i = 0; i < INITIAL_COUNT; i++) {
            estimator.add(String.valueOf(i));
        }

        expectedEstimateA = estimator.estimate();
        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HecatonInstance[] firstBrain, HecatonInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HecatonInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        estimatorA1 = firstBrain[0].getCardinalityEstimator(estimatorNameA);
        estimatorA2 = secondBrain[0].getCardinalityEstimator(estimatorNameA);

        estimatorB2 = secondBrain[0].getCardinalityEstimator(estimatorNameB);
        for (int i = 0; i < INITIAL_COUNT; i++) {
            estimatorB2.add(String.valueOf(i));
        }
        expectedEstimateB = estimatorB2.estimate();

        if (mergePolicyClass == DiscardMergePolicy.class) {
            onAfterSplitDiscardMergePolicy();
        } else if (mergePolicyClass == HyperLogLogMergePolicy.class) {
            onAfterSplitHyperLogLogMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            onAfterSplitPassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            onAfterSplitPutIfAbsentMergePolicy();
        } else {
            fail();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HecatonInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        estimatorB1 = instances[0].getCardinalityEstimator(estimatorNameB);

        backupEstimateA = getBackupEstimate(instances, estimatorA1);
        backupEstimateB = getBackupEstimate(instances, estimatorB1);

        if (mergePolicyClass == DiscardMergePolicy.class) {
            onAfterMergeDiscardMergePolicy();
        } else if (mergePolicyClass == HyperLogLogMergePolicy.class) {
            onAfterMergeHyperLogLogMergePolicy(instances);
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            onAfterMergePassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            onAfterMergePutIfAbsentMergePolicy();
        } else {
            fail();
        }
    }

    private void onAfterSplitDiscardMergePolicy() {
        // we should not have these items in the merged estimator, since they are in the smaller cluster only
        for (int i = INITIAL_COUNT; i < TOTAL_COUNT; i++) {
            estimatorA2.add(String.valueOf(i));
            estimatorB2.add(String.valueOf(i));
        }
    }

    private void onAfterMergeDiscardMergePolicy() {
        assertEquals(expectedEstimateA, estimatorA1.estimate());
        assertEquals(expectedEstimateA, estimatorA2.estimate());
        assertEquals(expectedEstimateA, backupEstimateA);

        assertEquals(0, estimatorB1.estimate());
        assertEquals(0, estimatorB2.estimate());
        assertEquals(0, backupEstimateB);
    }

    private void onAfterSplitHyperLogLogMergePolicy() {
        int firstBrainBump = INITIAL_COUNT + EXTRA_COUNT;

        for (int i = INITIAL_COUNT; i < firstBrainBump; i++) {
            estimatorA1.add(String.valueOf(i));
        }
        for (int i = INITIAL_COUNT; i < TOTAL_COUNT; i++) {
            estimatorA2.add(String.valueOf(i));
            estimatorB2.add(String.valueOf(i));
        }
    }

    private void onAfterMergeHyperLogLogMergePolicy(HecatonInstance[] instances) {
        CardinalityEstimator expectedEstimator = instances[0].getCardinalityEstimator("expectedEstimator");
        for (int i = 0; i < TOTAL_COUNT; i++) {
            expectedEstimator.add(String.valueOf(i));
        }
        long expectedValue = expectedEstimator.estimate();

        assertEquals(expectedValue, estimatorA1.estimate());
        assertEquals(expectedValue, estimatorA2.estimate());
        assertEquals(expectedValue, backupEstimateA);

        assertEquals(expectedValue, estimatorB2.estimate());
        assertEquals(expectedValue, backupEstimateB);
    }

    private void onAfterSplitPassThroughMergePolicy() {
        // we should not lose the additional values from estimatorA2 and estimatorB2
        for (int i = INITIAL_COUNT; i < TOTAL_COUNT; i++) {
            estimatorA2.add(String.valueOf(i));
            estimatorB2.add(String.valueOf(i));
        }

        expectedEstimateA = estimatorA2.estimate();
        expectedEstimateB = estimatorB2.estimate();
    }

    private void onAfterMergePassThroughMergePolicy() {
        assertEquals(expectedEstimateA, estimatorA1.estimate());
        assertEquals(expectedEstimateA, estimatorA2.estimate());
        assertEquals(expectedEstimateA, backupEstimateA);

        assertEquals(expectedEstimateB, estimatorB1.estimate());
        assertEquals(expectedEstimateB, estimatorB2.estimate());
        assertEquals(expectedEstimateB, backupEstimateB);
    }

    private void onAfterSplitPutIfAbsentMergePolicy() {
        // we should not lose the additional values from estimatorB2, but from estimatorA2
        for (int i = INITIAL_COUNT; i < TOTAL_COUNT; i++) {
            estimatorA2.add(String.valueOf(i));
            estimatorB2.add(String.valueOf(i));
        }

        expectedEstimateB = estimatorB2.estimate();
    }

    private void onAfterMergePutIfAbsentMergePolicy() {
        assertEquals(expectedEstimateA, estimatorA1.estimate());
        assertEquals(expectedEstimateA, estimatorA2.estimate());
        assertEquals(expectedEstimateA, backupEstimateA);

        assertEquals(expectedEstimateB, estimatorB1.estimate());
        assertEquals(expectedEstimateB, estimatorB2.estimate());
        assertEquals(expectedEstimateB, backupEstimateB);
    }
}
