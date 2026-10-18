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

import uk.org.hecaton.cache.impl.HecatonServerCachingProvider;
import uk.org.hecaton.cache.jsr.JsrTestUtil;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MergePolicyConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.config.mergepolicies.ComplexCustomMergePolicy;
import uk.org.hecaton.spi.merge.MergingCosts;
import uk.org.hecaton.spi.merge.MergingExpirationTime;
import uk.org.hecaton.spi.merge.PutIfAbsentMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the integration of the {@link MergePolicyValidator}
 * into the proxy creation of split-brain capable data structures.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class MergePolicyValidatorCachingProviderIntegrationTest
        extends AbstractMergePolicyValidatorIntegrationTest {

    @BeforeClass
    public static void jsrSetup() {
        JsrTestUtil.setup();
    }

    @AfterClass
    public static void jsrTeardown() {
        JsrTestUtil.cleanup();
    }

    @Override
    protected void addConfig(Config config, String name, MergePolicyConfig mergePolicyConfig) {
    }

    private void getCache(String name, MergePolicyConfig mergePolicyConfig) {
        HecatonInstance hz = getHecatonInstance(name, mergePolicyConfig);
        HecatonServerCachingProvider cachingProvider = createServerCachingProvider(hz);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName(name);
        cacheConfig.setStatisticsEnabled(false);
        cacheConfig.getMergePolicyConfig().setPolicy(mergePolicyConfig.getPolicy());

        cacheManager.createCache(name, cacheConfig);
    }

    @Test
    public void testCache_withPutIfAbsentMergePolicy() {
        getCache("putIfAbsent", putIfAbsentMergePolicy);
    }

    @Test
    public void testCache_withHyperLogLogMergePolicy() {
        expectCardinalityEstimatorException(() -> getCache("cardinalityEstimator", hyperLogLogMergePolicy));
    }

    @Test
    public void testCache_withHigherHitsMergePolicy() {
        getCache("higherHits", higherHitsMergePolicy);
    }

    @Test
    public void testCache_withInvalidMergePolicy() {
        expectedInvalidMergePolicyException(() -> getCache("invalid", invalidMergePolicyConfig));
    }

    @Test
    public void testCache_withExpirationTimeMergePolicy() {
        getCache("expirationTime", expirationTimeMergePolicy);
    }

    /**
     * ICache provides only the required {@link MergingExpirationTime},
     * but not the required {@link MergingCosts} from the
     * {@link ComplexCustomMergePolicy}.
     * <p>
     * The thrown exception should contain the merge policy name
     * and the missing merge type.
     */
    @Test
    public void testCache_withComplexCustomMergePolicy() {
        assertThatThrownBy(() -> getCache("complexCustom", complexCustomMergePolicy))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(complexCustomMergePolicy.getPolicy())
                .hasMessageContaining(MergingCosts.class.getName());
    }

    /**
     * ICache provides only some of the required merge types
     * of {@link MapMergeTypes}.
     * <p>
     * The thrown exception should contain the merge policy name
     * and the missing merge type.
     */
    @Test
    public void testCache_withCustomMapMergePolicyNoTypeVariable() {
        assertThatThrownBy(() -> getCache("customMapNoTypeVariable", customMapMergePolicyNoTypeVariable))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(customMapMergePolicyNoTypeVariable.getPolicy())
                .hasMessageContaining(MapMergeTypes.class.getName());
    }

    /**
     * ICache provides only some of the required merge types
     * of {@link MapMergeTypes}.
     * <p>
     * The thrown exception should contain the merge policy name
     * and the missing merge type.
     */
    @Test
    public void testCache_withCustomMapMergePolicy() {
        assertThatThrownBy(() -> getCache("customMap", customMapMergePolicy))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(customMapMergePolicy.getPolicy())
                .hasMessageContaining(MapMergeTypes.class.getName());
    }

    @Test
    public void testCache_withLegacyPutIfAbsentMergePolicy() {
        MergePolicyConfig legacyMergePolicyConfig = new MergePolicyConfig()
                .setPolicy(PutIfAbsentMergePolicy.class.getName());

        getCache("legacyPutIfAbsent", legacyMergePolicyConfig);
    }

    @Override
    protected void expectCardinalityEstimatorException(ThrowingCallable toRun) {
        assertThatThrownBy(toRun)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CardinalityEstimator");
    }

    @Override
    protected void expectedInvalidMergePolicyException(ThrowingCallable toRun) {
        assertThatThrownBy(toRun)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(invalidMergePolicyConfig.getPolicy());
    }
}
