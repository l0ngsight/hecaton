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

package uk.org.hecaton.cluster;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class AutoDetectionJoinTest extends AbstractJoinTest {

    @Before
    @After
    public void killAllHecatonInstances() {
        HecatonInstanceFactory.terminateAll();
    }

    @Test
    @Category(QuickTest.class)
    public void defaultConfig() {
        testJoinEventually(new Config());
    }

    @Test
    public void interfacesEnabled() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface(pickLocalInetAddress().getHostAddress());
        testJoinEventually(config);
    }

    @Test
    public void differentClusterNames() {
        Config config1 = new Config();
        config1.setClusterName("cluster1");

        Config config2 = new Config();
        config2.setClusterName("cluster2");

        assertIndependentClusters(config1, config2);
    }

    @Test
    public void autoDetectionDisabled() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        assertIndependentClusters(config, config);
    }

    @Test
    public void notUsedWhenOtherDiscoveryEnabled() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);

        assertIndependentClusters(config, config);
    }


}
