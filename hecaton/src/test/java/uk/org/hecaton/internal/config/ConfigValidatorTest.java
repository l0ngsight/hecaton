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

import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.config.cp.CPSubsystemConfig;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.merge.HigherHitsMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicyProvider;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static uk.org.hecaton.config.InMemoryFormat.BINARY;
import static uk.org.hecaton.config.InMemoryFormat.NATIVE;
import static uk.org.hecaton.config.InMemoryFormat.OBJECT;
import static uk.org.hecaton.internal.config.ConfigValidator.checkCPSubsystemConfig;
import static uk.org.hecaton.internal.config.ConfigValidator.checkCacheConfig;
import static uk.org.hecaton.internal.config.ConfigValidator.checkMapConfig;
import static uk.org.hecaton.internal.config.ConfigValidator.checkNearCacheNativeMemoryConfig;
import static org.mockito.Mockito.when;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigValidatorTest extends HecatonTestSupport {

    private SplitBrainMergePolicyProvider splitBrainMergePolicyProvider;

    @Before
    public void setUp() {
        Config config = new Config();
        NodeEngine nodeEngine = Mockito.mock(NodeEngine.class);
        when(nodeEngine.getConfigClassLoader()).thenReturn(config.getClassLoader());

        splitBrainMergePolicyProvider = new SplitBrainMergePolicyProvider(nodeEngine.getConfigClassLoader());
        when(nodeEngine.getSplitBrainMergePolicyProvider()).thenReturn(splitBrainMergePolicyProvider);
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ConfigValidator.class);
    }

    @Test
    public void checkMapConfig_BINARY() {
        checkMapConfig(new Config(), getMapConfig(BINARY), splitBrainMergePolicyProvider);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkMapConfig_fails_with_merge_policy_which_requires_per_entry_stats_enabled() {
        checkMapConfig(new Config(), getMapConfig(BINARY).setPerEntryStatsEnabled(false),
                splitBrainMergePolicyProvider);
    }

    @Test
    public void checkMapConfig_OBJECT() {
        checkMapConfig(new Config(), getMapConfig(OBJECT), splitBrainMergePolicyProvider);
    }

    /**
     * Not supported in open source version, so test is expected to throw exception.
     */
    @Test(expected = InvalidConfigurationException.class)
    public void checkMapConfig_NATIVE() {
        checkMapConfig(new Config(), getMapConfig(NATIVE), splitBrainMergePolicyProvider);
    }

    /**
     * Not supported in open source version, so test is expected to throw exception.
     */
    @Test(expected = InvalidConfigurationException.class)
    public void checkMapConfig_TieredStore() {
        checkMapConfig(new Config(), getMapConfig(true), splitBrainMergePolicyProvider);
    }

    private MapConfig getMapConfig(InMemoryFormat inMemoryFormat) {
        MapConfig mapConfig = new MapConfig()
                .setInMemoryFormat(inMemoryFormat)
                .setPerEntryStatsEnabled(true);
        mapConfig.getMergePolicyConfig()
                .setPolicy(HigherHitsMergePolicy.class.getName());
        return mapConfig;
    }

    private MapConfig getMapConfig(boolean tieredStoreEnabled) {
        MapConfig mapConfig = new MapConfig();
        mapConfig.getTieredStoreConfig().setEnabled(tieredStoreEnabled);
        return mapConfig;
    }

    @Test
    public void checkCacheConfig_withEntryCountMaxSizePolicy_OBJECT() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig()
                .setInMemoryFormat(OBJECT)
                .setEvictionConfig(evictionConfig);

        checkCacheConfig(cacheSimpleConfig, splitBrainMergePolicyProvider);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkCacheConfig_withEntryCountMaxSizePolicy_NATIVE() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig()
                .setInMemoryFormat(NATIVE)
                .setEvictionConfig(evictionConfig);

        checkCacheConfig(cacheSimpleConfig, splitBrainMergePolicyProvider);
    }

    @Test
    public void checkNearCacheNativeMemoryConfig_shouldNotNeedNativeMemoryConfig_BINARY_onOS() {
        checkNearCacheNativeMemoryConfig(BINARY, null, false);
    }

    @Test
    public void checkNearCacheNativeMemoryConfig_shouldNotNeedNativeMemoryConfig_BINARY_onEE() {
        checkNearCacheNativeMemoryConfig(BINARY, null, true);
    }

    @Test
    public void checkNearCacheNativeMemoryConfig_shouldNotThrowExceptionWithoutNativeMemoryConfig_NATIVE_onOS() {
        checkNearCacheNativeMemoryConfig(NATIVE, null, false);
    }

    @Test
    public void checkNearCacheNativeMemoryConfig_shouldNotThrowExceptionWithNativeMemoryConfig_NATIVE_onEE() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true);

        checkNearCacheNativeMemoryConfig(NATIVE, nativeMemoryConfig, true);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkNearCacheNativeMemoryConfig_shouldThrowExceptionWithoutNativeMemoryConfig_NATIVE_onEE() {
        checkNearCacheNativeMemoryConfig(NATIVE, null, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationFails_whenGroupSizeSetCPMemberCountNotSet() {
        CPSubsystemConfig config = new CPSubsystemConfig();
        config.setGroupSize(3);

        checkCPSubsystemConfig(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationFails_whenGroupSizeGreaterThanCPMemberCount() {
        CPSubsystemConfig config = new CPSubsystemConfig();
        config.setGroupSize(5);
        config.setCPMemberCount(3);

        checkCPSubsystemConfig(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationFails_whenSessionHeartbeatIntervalGreaterThanSessionTTL() {
        CPSubsystemConfig config = new CPSubsystemConfig();
        config.setSessionTimeToLiveSeconds(5);
        config.setSessionHeartbeatIntervalSeconds(10);

        checkCPSubsystemConfig(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationFails_whenSessionTTLGreaterThanMissingCPMemberAutoRemovalSeconds() {
        CPSubsystemConfig config = new CPSubsystemConfig();
        config.setMissingCPMemberAutoRemovalSeconds(5);
        config.setSessionTimeToLiveSeconds(10);

        checkCPSubsystemConfig(config);
    }
}
