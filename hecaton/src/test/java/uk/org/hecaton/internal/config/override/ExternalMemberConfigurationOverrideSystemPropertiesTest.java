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

package uk.org.hecaton.internal.config.override;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Properties;

import static uk.org.hecaton.config.RestEndpointGroup.DATA;
import static uk.org.hecaton.config.RestEndpointGroup.PERSISTENCE;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExternalMemberConfigurationOverrideSystemPropertiesTest extends HecatonTestSupport {

    private static final Map<String, String> EMPTY_ENV_VARIABLES = emptyMap();

    @Test
    public void shouldExtractConfigFromSysProperties() {
        Config config = new Config();
        Properties systemProperties = new Properties();
        systemProperties.setProperty("hc.cluster-name", "test");
        systemProperties.setProperty("hc.network.join.autodetection.enabled", "false");
        systemProperties.setProperty("hc.executor-service.custom.pool-size", "42");
        new ExternalConfigurationOverride(EMPTY_ENV_VARIABLES, () -> systemProperties).overwriteMemberConfig(config);

        assertEquals("test", config.getClusterName());
        assertEquals(42, config.getExecutorConfig("custom").getPoolSize());
        assertFalse(config.getNetworkConfig().getJoin().isAutoDetectionEnabled());
    }

    @Test
    public void shouldHandleRestApiConfigFromSysProperties() {
        Config config = new Config();
        Properties systemProperties = new Properties();
        systemProperties.setProperty("hc.network.rest-api.enabled", "true");
        systemProperties.setProperty("hc.network.rest-api.endpoint-groups.DATA.enabled", "true");
        systemProperties.setProperty("hc.network.rest-api.endpoint-groups.hot_restart.enabled", "true");

        new ExternalConfigurationOverride(EMPTY_ENV_VARIABLES, () -> systemProperties).overwriteMemberConfig(config);

        assertTrue(config.getNetworkConfig().getRestApiConfig().isEnabled());
        assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(DATA));
        assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(PERSISTENCE));
    }

    @Test
    public void shouldHandleRestApiConfigFromSysProperties_whenPersistenceEnabled() {
        Config config = new Config();
        Properties systemProperties = new Properties();
        systemProperties.setProperty("hc.network.rest-api.enabled", "true");
        systemProperties.setProperty("hc.network.rest-api.endpoint-groups.DATA.enabled", "true");
        systemProperties.setProperty("hc.network.rest-api.endpoint-groups.persistence.enabled", "true");

        new ExternalConfigurationOverride(EMPTY_ENV_VARIABLES, () -> systemProperties).overwriteMemberConfig(config);

        assertTrue(config.getNetworkConfig().getRestApiConfig().isEnabled());
        assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(DATA));
        assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(PERSISTENCE));
    }

    @Test
    public void shouldHandleRestApiConfigFromSysProperties_when_bothPersistenceAndHotRestartAreEnabled() {
        Config config = new Config();
        Properties systemProperties = new Properties();
        systemProperties.setProperty("hc.network.rest-api.enabled", "true");
        systemProperties.setProperty("hc.network.rest-api.endpoint-groups.DATA.enabled", "true");
        systemProperties.setProperty("hc.network.rest-api.endpoint-groups.hot_restart.enabled", "true");
        systemProperties.setProperty("hc.network.rest-api.endpoint-groups.persistence.enabled", "true");

        new ExternalConfigurationOverride(EMPTY_ENV_VARIABLES, () -> systemProperties).overwriteMemberConfig(config);

        assertTrue(config.getNetworkConfig().getRestApiConfig().isEnabled());
        assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(DATA));
        assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(PERSISTENCE));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void shouldHandleRestApiConfigFromSysPropertiesInvalidEntry() {
        Config config = new Config();
        Properties systemProperties = new Properties();
        systemProperties.setProperty("hc.network.rest-api.enabled", "true");
        systemProperties.setProperty("hc.network.rest-api.endpoint-groups.fooo.enabled", "true");
        systemProperties.setProperty("hc.network.rest-api.endpoint-groups.HOT_RESTART.enabled", "true");

        new ExternalConfigurationOverride(EMPTY_ENV_VARIABLES, () -> systemProperties).overwriteMemberConfig(config);
    }
}
