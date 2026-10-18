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

package uk.org.hecaton.client.internal.metrics;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientMetricsConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.MetricsConfig;
import uk.org.hecaton.internal.metrics.MetricsRegistry;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.internal.metrics.ProbeLevel.DEBUG;
import static uk.org.hecaton.internal.metrics.ProbeLevel.INFO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMetricsPropertiesTest extends HecatonTestSupport {
    private TestHecatonFactory factory;
    private ClientConfig clientConfig;
    private ClientMetricsConfig originalMetricsConfig;

    @Before
    public void before() {
        clientConfig = new ClientConfig();
        originalMetricsConfig = clientConfig.getMetricsConfig();
        factory = new TestHecatonFactory();
        factory.newHecatonInstance(smallInstanceConfig());
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testSystemPropertiesOverrideConfig() {
        // setting non-defaults
        System.setProperty(ClientProperty.METRICS_ENABLED.getName(), "false");
        System.setProperty(ClientProperty.METRICS_JMX_ENABLED.getName(), "false");
        System.setProperty(ClientProperty.METRICS_COLLECTION_FREQUENCY.getName(), "24");

        HecatonClientProxy client = createClient();
        ClientConfig clientConfig = client.getClientConfig();

        ClientMetricsConfig metricsConfig = clientConfig.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
        assertEquals(24, metricsConfig.getCollectionFrequencySeconds());

        // verify that the overridden config is used
        ClientMetricsConfig metricsConfigUsed = client.client.getClientStatisticsService().getMetricsConfig();
        assertSame(originalMetricsConfig, metricsConfigUsed);
    }

    @Test
    public void testInvalidSystemPropertiesIgnored() {
        // setting non-defaults
        System.setProperty(ClientProperty.METRICS_ENABLED.getName(), "invalid");
        System.setProperty(ClientProperty.METRICS_JMX_ENABLED.getName(), "invalid");
        System.setProperty(ClientProperty.METRICS_COLLECTION_FREQUENCY.getName(), "invalid");

        HecatonClientProxy client = createClient();
        ClientConfig config = client.getClientConfig();

        ClientMetricsConfig defaultConfig = new ClientMetricsConfig();

        // booleans result in false values even though they're "invalid"
        // therefore, all boolean config fields are set to false
        ClientMetricsConfig metricsConfig = config.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
        assertEquals(defaultConfig.getCollectionFrequencySeconds(), metricsConfig.getCollectionFrequencySeconds());

        // verify that the overridden config is used
        ClientMetricsConfig metricsConfigUsed = client.client.getClientStatisticsService().getMetricsConfig();
        assertSame(originalMetricsConfig, metricsConfigUsed);
    }

    @Test
    public void testConfigPropertiesOverrideConfig() {
        // setting non-defaults
        clientConfig.setProperty(ClientProperty.METRICS_ENABLED.getName(), "false");
        clientConfig.setProperty(ClientProperty.METRICS_JMX_ENABLED.getName(), "false");
        clientConfig.setProperty(ClientProperty.METRICS_COLLECTION_FREQUENCY.getName(), "24");
        HecatonClientProxy client = createClient();

        assertFalse(originalMetricsConfig.isEnabled());
        assertFalse(originalMetricsConfig.getJmxConfig().isEnabled());
        assertEquals(24, originalMetricsConfig.getCollectionFrequencySeconds());

        // verify that the overridden config is used
        ClientMetricsConfig metricsConfigUsed = client.client.getClientStatisticsService().getMetricsConfig();
        assertSame(originalMetricsConfig, metricsConfigUsed);
    }

    @Test
    public void testInvalidConfigPropertiesIgnored() {
        // setting non-defaults
        clientConfig.setProperty(ClientProperty.METRICS_ENABLED.getName(), "invalid");
        clientConfig.setProperty(ClientProperty.METRICS_JMX_ENABLED.getName(), "invalid");
        clientConfig.setProperty(ClientProperty.METRICS_COLLECTION_FREQUENCY.getName(), "invalid");

        HecatonClientProxy client = createClient();

        MetricsConfig defaultConfig = new MetricsConfig();

        // booleans result in false values even though they're "invalid"
        // therefore, all boolean config fields are set to false
        assertFalse(originalMetricsConfig.isEnabled());
        assertFalse(originalMetricsConfig.getJmxConfig().isEnabled());
        assertEquals(defaultConfig.getCollectionFrequencySeconds(), originalMetricsConfig.getCollectionFrequencySeconds());

        // verify that the overridden config is used
        ClientMetricsConfig metricsConfigUsed = client.client.getClientStatisticsService().getMetricsConfig();
        assertSame(originalMetricsConfig, metricsConfigUsed);
    }

    @Test
    public void testDebugMetricsSysPropNotSet() {
        MetricsRegistry metricsRegistry = createClient().client.getMetricsRegistry();

        assertEquals(INFO, metricsRegistry.minimumLevel());
    }

    @Test
    public void testDebugMetricsSysPropDisabled() {
        System.setProperty(ClientProperty.METRICS_DEBUG.getName(), "false");
        MetricsRegistry metricsRegistry = createClient().client.getMetricsRegistry();

        assertEquals(INFO, metricsRegistry.minimumLevel());
    }

    @Test
    public void testDebugMetricsSysPropEnabled() {
        System.setProperty(ClientProperty.METRICS_DEBUG.getName(), "true");
        MetricsRegistry metricsRegistry = createClient().client.getMetricsRegistry();

        assertEquals(DEBUG, metricsRegistry.minimumLevel());
    }

    @Test
    public void testDeprecatedPropertiesStillEffective() {
        // setting non-defaults
        clientConfig.setProperty(ClientProperty.STATISTICS_ENABLED.getName(), "false");
        clientConfig.setProperty(ClientProperty.STATISTICS_PERIOD_SECONDS.getName(), "24");

        HecatonClientProxy client = createClient();
        ClientConfig clientConfig = client.getClientConfig();

        ClientMetricsConfig metricsConfig = clientConfig.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertEquals(24, metricsConfig.getCollectionFrequencySeconds());
    }

    @Test
    public void testDeprecatedPropertiesIgnored_whenNewPropertiesGiven() {
        clientConfig.setProperty(ClientProperty.STATISTICS_ENABLED.getName(), "true");
        clientConfig.setProperty(ClientProperty.STATISTICS_PERIOD_SECONDS.getName(), "24");

        clientConfig.setProperty(ClientProperty.METRICS_ENABLED.getName(), "false");
        clientConfig.setProperty(ClientProperty.METRICS_COLLECTION_FREQUENCY.getName(), "30");

        HecatonClientProxy client = createClient();
        ClientConfig clientConfig = client.getClientConfig();

        ClientMetricsConfig metricsConfig = clientConfig.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertEquals(30, metricsConfig.getCollectionFrequencySeconds());
    }

    private HecatonClientProxy createClient() {
        return (HecatonClientProxy) factory.newHecatonClient(clientConfig);
    }

}
