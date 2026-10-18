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

package uk.org.hecaton.kubernetes;

import uk.org.hecaton.config.properties.PropertyDefinition;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.NoLogFactory;
import uk.org.hecaton.spi.discovery.DiscoveryNode;
import uk.org.hecaton.spi.discovery.DiscoveryStrategy;
import uk.org.hecaton.spi.discovery.DiscoveryStrategyFactory.DiscoveryStrategyLevel;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonKubernetesDiscoveryStrategyFactoryTest {

    private static final ILogger LOGGER = new NoLogFactory().getLogger("no");
    private static final String API_TOKEN = "token";
    private static final String CA_CERTIFICATE = "ca-certificate";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void checkDiscoveryStrategyType() {
        HecatonKubernetesDiscoveryStrategyFactory factory = new HecatonKubernetesDiscoveryStrategyFactory();
        Class<? extends DiscoveryStrategy> strategyType = factory.getDiscoveryStrategyType();
        assertEquals(HecatonKubernetesDiscoveryStrategy.class.getCanonicalName(), strategyType.getCanonicalName());
    }

    @Test
    public void checkProperties() {
        HecatonKubernetesDiscoveryStrategyFactory factory = new HecatonKubernetesDiscoveryStrategyFactory();
        Collection<PropertyDefinition> propertyDefinitions = factory.getConfigurationProperties();
        assertTrue(propertyDefinitions.contains(KubernetesProperties.SERVICE_DNS));
        assertTrue(propertyDefinitions.contains(KubernetesProperties.SERVICE_PORT));
    }

    @Test
    public void createDiscoveryStrategy() {
        testCreateDiscoveryStrategy(mock(DiscoveryNode.class));
    }

    @Test
    public void createDiscoveryStrategy_whenDiscoveryNodeIsNull() {
        testCreateDiscoveryStrategy(null);
    }

    @Test
    public void autoDetection() throws Exception {
        // given
        String token = tempFolder.newFile("some-token").getAbsolutePath();
        HecatonKubernetesDiscoveryStrategyFactory factory = new HecatonKubernetesDiscoveryStrategyFactory(token);

        // when & then
        assertTrue(factory.tokenFileExists());
        assertEquals(DiscoveryStrategyLevel.PLATFORM, factory.discoveryStrategyLevel());
    }

    private void testCreateDiscoveryStrategy(DiscoveryNode node) {
        HashMap<String, Comparable> properties = new HashMap<>();
        properties.put(KubernetesProperties.KUBERNETES_API_TOKEN.key(), API_TOKEN);
        properties.put(KubernetesProperties.KUBERNETES_CA_CERTIFICATE.key(), CA_CERTIFICATE);
        properties.put(String.valueOf(KubernetesProperties.SERVICE_PORT), 333);
        properties.put(KubernetesProperties.NAMESPACE.key(), "default");
        HecatonKubernetesDiscoveryStrategyFactory factory = new HecatonKubernetesDiscoveryStrategyFactory();
        DiscoveryStrategy strategy = factory.newDiscoveryStrategy(node, LOGGER, properties);
        assertTrue(strategy instanceof HecatonKubernetesDiscoveryStrategy);
        strategy.start();
        strategy.destroy();
    }
}
