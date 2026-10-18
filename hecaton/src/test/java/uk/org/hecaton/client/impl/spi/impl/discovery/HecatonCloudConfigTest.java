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

package uk.org.hecaton.client.impl.spi.impl.discovery;


import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonCloudConfigTest extends ClientTestSupport {

    @Test
    public void testCustomCloudUrlEndpoint() {
        ClientConfig config = new ClientConfig();
        String token = "TOKEN";
        config.setProperty(ClientProperty.HECATON_CLOUD_DISCOVERY_TOKEN.getName(), token);
        config.setProperty(HecatonCloudDiscovery.CLOUD_URL_BASE_PROPERTY.getName(), "https://dev.hazelcast.cloud");
        HecatonProperties hecatonProperties = new HecatonProperties(config.getProperties());
        String cloudUrlBase = hecatonProperties.getString(HecatonCloudDiscovery.CLOUD_URL_BASE_PROPERTY);
        String urlEndpoint = HecatonCloudDiscovery.createUrlEndpoint(cloudUrlBase, token);
        assertEquals("https://dev.hazelcast.cloud/cluster/discovery?token=TOKEN", urlEndpoint);
    }

    @Test
    public void testDefaultCloudUrlEndpoint() {
        ClientConfig config = new ClientConfig();
        String token = "TOKEN";
        config.setProperty(ClientProperty.HECATON_CLOUD_DISCOVERY_TOKEN.getName(), token);
        HecatonProperties hecatonProperties = new HecatonProperties(config.getProperties());
        String cloudUrlBase = hecatonProperties.getString(HecatonCloudDiscovery.CLOUD_URL_BASE_PROPERTY);
        String urlEndpoint = HecatonCloudDiscovery.createUrlEndpoint(cloudUrlBase, token);
        assertEquals("https://api.cloud.hazelcast.com/cluster/discovery?token=TOKEN", urlEndpoint);
    }

}
