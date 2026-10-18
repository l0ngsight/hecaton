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

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static uk.org.hecaton.client.config.ClientConnectionStrategyConfig.ReconnectMode.ASYNC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExternalClientConfigurationOverrideTest extends HecatonTestSupport {

    @Test
    public void shouldExtractConfigFromEnv() {
        ClientConfig config = new ClientConfig();

        Map<String, String> systemEnvs = new HashMap<>();
        systemEnvs.put("HCCLIENT_INSTANCENAME", "test");
        systemEnvs.put("HCCLIENT_NETWORK_AUTODETECTION_ENABLED", "false");

        new ExternalConfigurationOverride(systemEnvs, System::getProperties).overwriteClientConfig(config);
        assertEquals("test", config.getInstanceName());
        assertFalse(config.getNetworkConfig().isAutoDetectionEnabled());
    }

    @Test
    public void shouldHandleConnectionStrategy() {
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig()
                .setReconnectMode(ASYNC)
                .setAsyncStart(true);

        Map<String, String> systemEnvs = new HashMap<>();
        systemEnvs.put("HCCLIENT_CONNECTIONSTRATEGY_ASYNCSTART", "false");

        new ExternalConfigurationOverride(systemEnvs, System::getProperties).overwriteClientConfig(config);

        assertFalse(config.getConnectionStrategyConfig().isAsyncStart());
        assertEquals(ASYNC, config.getConnectionStrategyConfig().getReconnectMode());
    }
    @Test
    public void shouldHandleCustomPropertiesConfig() {
        ClientConfig config = new ClientConfig();

        Map<String, String> systemEnvs = new HashMap<>();
        systemEnvs.put("HCCLIENT_PROPERTIES_foo", "bar");

        new ExternalConfigurationOverride(systemEnvs, System::getProperties).overwriteClientConfig(config);

        assertEquals("bar", config.getProperty("foo"));
    }
}
