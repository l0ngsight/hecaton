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
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.MergePolicyConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.config.mergepolicies.ComplexCustomMergePolicy;
import uk.org.hecaton.internal.config.mergepolicies.CustomMapMergePolicy;
import uk.org.hecaton.internal.config.mergepolicies.CustomMapMergePolicyNoTypeVariable;
import uk.org.hecaton.internal.config.mergepolicies.LastStoredTimeMergePolicy;
import uk.org.hecaton.internal.config.mergepolicies.LastStoredTimeMergePolicyNoTypeVariable;
import uk.org.hecaton.spi.merge.DiscardMergePolicy;
import uk.org.hecaton.spi.merge.ExpirationTimeMergePolicy;
import uk.org.hecaton.spi.merge.HigherHitsMergePolicy;
import uk.org.hecaton.spi.merge.HyperLogLogMergePolicy;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.spi.merge.PutIfAbsentMergePolicy;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Before;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the integration of the {@link MergePolicyValidator}
 * into the proxy creation of split-brain capable data structures.
 */
public abstract class AbstractMergePolicyValidatorIntegrationTest extends HecatonTestSupport {

    protected MergePolicyConfig putIfAbsentMergePolicy;
    protected MergePolicyConfig hyperLogLogMergePolicy;
    protected MergePolicyConfig higherHitsMergePolicy;
    protected MergePolicyConfig invalidMergePolicyConfig;
    protected MergePolicyConfig expirationTimeMergePolicy;
    protected MergePolicyConfig lastStoredTimeMergePolicy;
    protected MergePolicyConfig lastStoredTimeMergePolicyNoTypeVariable;
    protected MergePolicyConfig complexCustomMergePolicy;
    protected MergePolicyConfig customMapMergePolicy;
    protected MergePolicyConfig customMapMergePolicyNoTypeVariable;
    protected MergePolicyConfig passThroughMergePolicy;
    protected MergePolicyConfig discardMergePolicy;

    private TestHecatonInstanceFactory factory;

    @Before
    public final void setUp() {
        putIfAbsentMergePolicy = new MergePolicyConfig()
                .setPolicy(PutIfAbsentMergePolicy.class.getSimpleName());
        passThroughMergePolicy = new MergePolicyConfig()
                .setPolicy(PassThroughMergePolicy.class.getSimpleName());
        discardMergePolicy = new MergePolicyConfig()
                .setPolicy(DiscardMergePolicy.class.getSimpleName());
        hyperLogLogMergePolicy = new MergePolicyConfig()
                .setPolicy(HyperLogLogMergePolicy.class.getName());
        higherHitsMergePolicy = new MergePolicyConfig()
                .setPolicy(HigherHitsMergePolicy.class.getSimpleName());
        invalidMergePolicyConfig = new MergePolicyConfig()
                .setPolicy("InvalidMergePolicyClass");
        expirationTimeMergePolicy = new MergePolicyConfig()
                .setPolicy(ExpirationTimeMergePolicy.class.getName());
        lastStoredTimeMergePolicy = new MergePolicyConfig()
                .setPolicy(LastStoredTimeMergePolicy.class.getName());
        lastStoredTimeMergePolicyNoTypeVariable = new MergePolicyConfig()
                .setPolicy(LastStoredTimeMergePolicyNoTypeVariable.class.getName());
        complexCustomMergePolicy = new MergePolicyConfig()
                .setPolicy(ComplexCustomMergePolicy.class.getName());
        customMapMergePolicy = new MergePolicyConfig()
                .setPolicy(CustomMapMergePolicy.class.getName());
        customMapMergePolicyNoTypeVariable = new MergePolicyConfig()
                .setPolicy(CustomMapMergePolicyNoTypeVariable.class.getName());

        factory = createHecatonInstanceFactory();
    }

    protected abstract void addConfig(Config config, String name, MergePolicyConfig mergePolicyConfig);

    protected HecatonInstance getHecatonInstance(String name, MergePolicyConfig mergePolicyConfig) {
        Config config = smallInstanceConfig();
        addConfig(config, name, mergePolicyConfig);

        return factory.newHecatonInstance(config);
    }

    protected void expectCardinalityEstimatorException(ThrowingCallable toRun) {
        assertThatThrownBy(toRun)
                .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining("CardinalityEstimator");
    }

    protected void expectedHigherHitsException(ThrowingCallable toRun) {
        assertThatThrownBy(toRun)
                .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining(higherHitsMergePolicy.getPolicy());
    }

    protected void expectedInvalidMergePolicyException(ThrowingCallable toRun) {
        assertThatThrownBy(toRun)
                .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining(invalidMergePolicyConfig.getPolicy());
    }

    protected void expectedMapStatisticsDisabledException(MergePolicyConfig mergePolicyConfig, ThrowingCallable toRun) {
        assertThatThrownBy(toRun)
                .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining(mergePolicyConfig.getPolicy())
                                .hasMessageContaining("perEntryStatsEnabled field of map-config");
    }

    protected void expectInvalidConfigurationException(ThrowingCallable toRun, MergePolicyConfig mergePolicyConfig) {
        assertThatThrownBy(toRun)
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining(mergePolicyConfig.getPolicy());
    }
}
