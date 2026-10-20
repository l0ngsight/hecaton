/*
 * Copyright (c) 2026, The Hecaton Authors.
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

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.XmlClientConfigBuilder;
import uk.org.hecaton.client.config.YamlClientConfigBuilder;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

/**
 * Configs that still use the cloud-provider discovery Hecaton removed (D9) fail to load with
 * a message that says what to use instead (spec NG6).
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RemovedCloudDiscoveryConfigTest {

    private static final String MESSAGE = "Cloud-provider discovery is not supported in Hecaton; use tcp-ip, multicast,"
            + " kubernetes or a Discovery SPI implementation";
    private static final List<String> TAGS = List.of("aws", "gcp", "azure", "eureka");

    @Test
    public void memberJoin_xml() {
        for (String tag : TAGS) {
            String xml = "<hecaton xmlns=\"https://hecaton.org.uk/schema/config\">\n"
                    + "  <network><join><" + tag + " enabled=\"false\"/></join></network>\n"
                    + "</hecaton>\n";
            assertRejected(tag, () -> Config.loadFromString(xml));
        }
    }

    @Test
    public void memberJoin_yaml() {
        for (String tag : TAGS) {
            String yaml = "hecaton:\n"
                    + "  network:\n"
                    + "    join:\n"
                    + "      " + tag + ":\n"
                    + "        enabled: false\n";
            assertRejected(tag, () -> Config.loadFromString(yaml));
        }
    }

    @Test
    public void wanBatchPublisher_xml() {
        for (String tag : TAGS) {
            String xml = "<hecaton xmlns=\"https://hecaton.org.uk/schema/config\">\n"
                    + "  <wan-replication name=\"wan\">\n"
                    + "    <batch-publisher>\n"
                    + "      <cluster-name>target</cluster-name>\n"
                    + "      <" + tag + " enabled=\"true\"/>\n"
                    + "    </batch-publisher>\n"
                    + "  </wan-replication>\n"
                    + "</hecaton>\n";
            assertRejected(tag, () -> Config.loadFromString(xml));
        }
    }

    @Test
    public void client_xml() {
        for (String tag : List.of("aws", "gcp", "azure", "eureka", "hecaton-cloud")) {
            String xml = "<hecaton-client xmlns=\"https://hecaton.org.uk/schema/client-config\">\n"
                    + "  <network><" + tag + " enabled=\"true\"/></network>\n"
                    + "</hecaton-client>\n";
            assertRejected(tag, () -> new XmlClientConfigBuilder(new ByteArrayInputStream(xml.getBytes(UTF_8))).build());
        }
    }

    @Test
    public void client_yaml() {
        for (String tag : List.of("aws", "gcp", "azure", "eureka", "hecaton-cloud")) {
            String yaml = "hecaton-client:\n"
                    + "  network:\n"
                    + "    " + tag + ":\n"
                    + "      enabled: true\n";
            assertRejected(tag, () -> new YamlClientConfigBuilder(new ByteArrayInputStream(yaml.getBytes(UTF_8))).build());
        }
    }

    @Test
    public void kubernetes_stillSupported() {
        String xml = "<hecaton xmlns=\"https://hecaton.org.uk/schema/config\">\n"
                + "  <network><join><multicast enabled=\"false\"/><kubernetes enabled=\"true\"/></join></network>\n"
                + "</hecaton>\n";
        Config config = Config.loadFromString(xml);
        assertTrue(config.getNetworkConfig().getJoin().getKubernetesConfig().isEnabled());
        ClientConfig clientConfig = new XmlClientConfigBuilder(new ByteArrayInputStream(
                ("<hecaton-client xmlns=\"https://hecaton.org.uk/schema/client-config\">\n"
                        + "  <network><kubernetes enabled=\"true\"/></network>\n"
                        + "</hecaton-client>\n").getBytes(UTF_8))).build();
        assertTrue(clientConfig.getNetworkConfig().getKubernetesConfig().isEnabled());
    }

    private static void assertRejected(String tag, ThrowingCallable load) {
        assertThatThrownBy(load)
                .as(tag)
                .hasStackTraceContaining("'" + tag + "': " + MESSAGE);
    }
}
