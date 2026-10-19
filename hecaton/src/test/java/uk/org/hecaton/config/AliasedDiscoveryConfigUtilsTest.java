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

package uk.org.hecaton.config;

import uk.org.hecaton.internal.config.AliasedDiscoveryConfigUtils;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AliasedDiscoveryConfigUtilsTest {

    @Test
    public void supports() {
        assertTrue(AliasedDiscoveryConfigUtils.supports("kubernetes"));
        assertFalse(AliasedDiscoveryConfigUtils.supports("unknown"));
    }

    @Test
    public void removedCloudProviderTags_areRejected() {
        for (String tag : List.of("aws", "gcp", "azure", "eureka", "hecaton-cloud")) {
            assertThatThrownBy(() -> AliasedDiscoveryConfigUtils.supports(tag))
                    .isInstanceOf(InvalidConfigurationException.class)
                    .hasMessageContaining("'" + tag + "': Cloud-provider discovery is not supported in Hecaton");
            assertThatThrownBy(() -> AliasedDiscoveryConfigUtils.newConfigFor(tag))
                    .isInstanceOf(InvalidConfigurationException.class);
            assertThatThrownBy(() -> AliasedDiscoveryConfigUtils.getConfigByTag(new JoinConfig(), tag))
                    .isInstanceOf(InvalidConfigurationException.class);
        }
    }

    @Test
    public void tagFor() {
        assertEquals("kubernetes", AliasedDiscoveryConfigUtils.tagFor(new KubernetesConfig()));
        assertEquals("kubernetes", AliasedDiscoveryConfigUtils.tagFor(new KubernetesConfig() {
        }));
        assertNull(AliasedDiscoveryConfigUtils.tagFor(new DummyAliasedDiscoveryConfig(null)));
    }

    @Test
    public void createDiscoveryStrategyConfigsFromJoinConfig() {
        // given
        JoinConfig config = new JoinConfig();
        config.getKubernetesConfig().setEnabled(true);

        // when
        List<DiscoveryStrategyConfig> result = AliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(config);

        // then
        assertEquals(1, result.size());
        assertEquals("uk.org.hecaton.kubernetes.HecatonKubernetesDiscoveryStrategy", result.get(0).getClassName());
    }

    @Test
    public void createDiscoveryStrategyConfigsFromWanPublisherConfig() {
        // given
        WanBatchPublisherConfig config = new WanBatchPublisherConfig();
        config.getKubernetesConfig().setEnabled(true);

        // when
        List<DiscoveryStrategyConfig> result = AliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(config);

        // then
        assertEquals(1, result.size());
        assertEquals("uk.org.hecaton.kubernetes.HecatonKubernetesDiscoveryStrategy", result.get(0).getClassName());
    }

    @Test
    public void map() {
        // given
        List<AliasedDiscoveryConfig<?>> aliasedDiscoveryConfigs = new ArrayList<>();
        aliasedDiscoveryConfigs.add(new KubernetesConfig().setEnabled(true).setProperty("namespace", "hecaton-33")
                .setProperty("service-name", "my-service"));
        aliasedDiscoveryConfigs.add(new KubernetesConfig().setEnabled(false).setProperty("namespace", "disabled"));

        // when
        List<DiscoveryStrategyConfig> discoveryConfigs = AliasedDiscoveryConfigUtils.map(aliasedDiscoveryConfigs);

        // then
        assertEquals(1, discoveryConfigs.size());
        DiscoveryStrategyConfig kubernetesConfig = discoveryConfigs.get(0);
        assertEquals("uk.org.hecaton.kubernetes.HecatonKubernetesDiscoveryStrategy", kubernetesConfig.getClassName());
        assertEquals("hecaton-33", kubernetesConfig.getProperties().get("namespace"));
        assertEquals("my-service", kubernetesConfig.getProperties().get("service-name"));
    }

    @Test
    public void skipNotEnabledConfigs() {
        // given
        List<AliasedDiscoveryConfig<?>> configs = new ArrayList<>();
        configs.add(new KubernetesConfig().setEnabled(false));

        // when
        List<DiscoveryStrategyConfig> discoveryConfigs = AliasedDiscoveryConfigUtils.map(configs);

        // then
        assertTrue(discoveryConfigs.isEmpty());
    }

    @Test
    public void skipPropertyWithNullKey() {
        // given
        List<AliasedDiscoveryConfig<?>> configs = new ArrayList<>();
        configs.add(new KubernetesConfig().setEnabled(true).setProperty(null, "value"));

        // when
        List<DiscoveryStrategyConfig> discoveryConfigs = AliasedDiscoveryConfigUtils.map(configs);

        // then
        assertTrue(discoveryConfigs.get(0).getProperties().isEmpty());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void validateUnknownEnvironments() {
        // given
        AliasedDiscoveryConfig aliasedDiscoveryConfig = new DummyAliasedDiscoveryConfig("invalid-tag") {
        }.setEnabled(true);
        List<AliasedDiscoveryConfig<?>> configs = new ArrayList<>();
        configs.add(aliasedDiscoveryConfig);

        // when
        AliasedDiscoveryConfigUtils.map(configs);

        // then
        // throws exception
    }

    @Test
    public void getConfigByTagFromJoinConfig() {
        // given
        JoinConfig config = new JoinConfig();

        // when
        AliasedDiscoveryConfig result = AliasedDiscoveryConfigUtils.getConfigByTag(config, "kubernetes");

        // then
        assertEquals(KubernetesConfig.class, result.getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getConfigByInvalidTagFromJoinConfig() {
        // given
        JoinConfig config = new JoinConfig();

        // when
        AliasedDiscoveryConfigUtils.getConfigByTag(config, "unknown");

        // then
        // throw exception
    }

    @Test
    public void getConfigByTagFromWanPublisherConfig() {
        // given
        WanBatchPublisherConfig config = new WanBatchPublisherConfig();

        // when
        AliasedDiscoveryConfig result = AliasedDiscoveryConfigUtils.getConfigByTag(config, "kubernetes");

        // then
        assertEquals(KubernetesConfig.class, result.getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getConfigByInvalidTagFromWanPublisherConfig() {
        // given
        WanBatchPublisherConfig config = new WanBatchPublisherConfig();

        // when
        AliasedDiscoveryConfigUtils.getConfigByTag(config, "unknown");

        // then
        // throw exception
    }

    @Test
    public void allUsePublicAddressTrue() {
        // given
        KubernetesConfig first = new KubernetesConfig().setEnabled(true).setUsePublicIp(true);
        KubernetesConfig second = new KubernetesConfig().setEnabled(true).setUsePublicIp(true);
        List<AliasedDiscoveryConfig<?>> configs = asList(first, second);

        // when
        boolean result = AliasedDiscoveryConfigUtils.allUsePublicAddress(configs);

        // then
        assertTrue(result);
    }

    @Test
    public void allUsePublicAddressFalse() {
        // given
        KubernetesConfig first = new KubernetesConfig().setEnabled(true).setUsePublicIp(true);
        KubernetesConfig second = new KubernetesConfig().setEnabled(true).setUsePublicIp(false);
        List<AliasedDiscoveryConfig<?>> configs = asList(first, second);

        // when
        boolean result = AliasedDiscoveryConfigUtils.allUsePublicAddress(configs);

        // then
        assertFalse(result);
    }

    @Test
    public void allUsePublicAddressEmpty() {
        // given
        List<AliasedDiscoveryConfig<?>> configs = new ArrayList<>();

        // when
        boolean result = AliasedDiscoveryConfigUtils.allUsePublicAddress(configs);

        // then
        assertFalse(result);
    }

    private static class DummyAliasedDiscoveryConfig extends AliasedDiscoveryConfig {
        protected DummyAliasedDiscoveryConfig(String tag) {
            super(tag);
        }

        @Override
        public int getClassId() {
            throw new UnsupportedOperationException("Deserialization not supported!");
        }
    }
}
