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

package uk.org.hecaton.client.config;

import uk.org.hecaton.internal.config.SchemaViolationConfigurationException;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class YamlClientFailoverConfigBuilderTest extends AbstractClientFailoverConfigBuilderTest {


    @Before
    public void init() throws Exception {
        URL schemaResource = YamlClientFailoverConfigBuilderTest.class.getClassLoader()
                                                                      .getResource("hecaton-client-failover-sample.yaml");
        fullClientConfig = new YamlClientFailoverConfigBuilder(schemaResource).build();
    }

    public void testNoHecatonClientFailoverRootElement() {
        String yaml = "try-count: 2";

        ClientFailoverConfig clientFailoverConfig = buildConfig(yaml);
        assertEquals(2, clientFailoverConfig.getTryCount());
    }

    @Test(expected = SchemaViolationConfigurationException.class)
    public void testExpectsAtLeastOneConfig() {
        String yaml = """
              hecaton-client-failover:
                clients: []
              """;

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testVariableReplacementFromProperties() {
        String yaml = """
                hecaton-client-failover:
                  clients:
                    - hecaton-client-c1.yaml
                    - hecaton-client-c2.yaml
                  try-count: ${try-count}""";

        Properties properties = new Properties();
        properties.setProperty("try-count", "11");
        ClientFailoverConfig config = buildConfig(yaml, properties);
        assertEquals(11, config.getTryCount());
    }

    @Override
    @Test
    public void testVariableReplacementFromSystemProperties() {
        String yaml = """
                hecaton-client-failover:
                  clients:
                    - hecaton-client-c1.yaml
                    - hecaton-client-c2.yaml
                  try-count: ${try-count}""";

        System.setProperty("try-count", "11");
        ClientFailoverConfig config = buildConfig(yaml);
        assertEquals(11, config.getTryCount());
    }

    @Test
    public void testEmptyYamlConfig() {
        String emptyYaml = "hecaton-client-failover:\n";
        ClientFailoverConfig emptyFailoverConfig = buildConfig(emptyYaml);
        ClientFailoverConfig defaultFailoverConfig = new ClientFailoverConfig();

        assertEquals(emptyFailoverConfig.getTryCount(), defaultFailoverConfig.getTryCount());
        HecatonTestSupport.assertContainsAll(emptyFailoverConfig.getClientConfigs(), defaultFailoverConfig.getClientConfigs());
    }

    @Override
    @Test
    public void testWithClasspathConfig() {
        ClientFailoverConfig config = new ClientFailoverClasspathYamlConfig("hecaton-client-failover-sample.yaml");
        assertSampleFailoverConfig(config);
    }

    @Override
    @Test
    public void testVariableReplacementFromSystemPropertiesWithClasspathConfig() {
        System.setProperty("try-count", "13");
        ClientFailoverConfig config = new ClientFailoverClasspathYamlConfig(
                "hecaton-client-failover-sample-with-variable.yaml");
        assertEquals(13, config.getTryCount());
    }

    @Override
    @Test
    public void loadingThroughSystemProperty_existingClasspathResource() {
        System.setProperty("hecaton.client.failover.config", "classpath:hecaton-client-failover-sample.yaml");

        ClientFailoverConfig config = buildConfig();
        assertSampleFailoverConfig(config);
    }

    @Override
    ClientFailoverConfig buildConfig() {
        return new YamlClientFailoverConfigBuilder().build();
    }

    public static ClientFailoverConfig buildConfig(String yaml) {
        return buildConfig(yaml, null);
    }

    private static ClientFailoverConfig buildConfig(String yaml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());
        YamlClientFailoverConfigBuilder configBuilder = new YamlClientFailoverConfigBuilder(bis);
        if (properties != null) {
            configBuilder.setProperties(properties);
        }
        return configBuilder.build();
    }
}
