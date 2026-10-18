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
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.instance.impl.TestUtil;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.OverridePropertyRule;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.HecatonTestSupport.assertClusterSize;
import static uk.org.hecaton.test.OverridePropertyRule.clear;
import static org.junit.Assert.assertTrue;

/**
 * Test that the Hecaton infrastructure can deal correctly with {@link uk.org.hecaton.internal.cluster.impl.ConfigCheck} violations.
 * Most of the actual cases are tested in the {@link uk.org.hecaton.cluster.ConfigCheckTest}. In this class we run a bunch
 * of integration tests to make sure that it really works like it is supposed to work.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class ClusterJoinConfigCheckTest {

    @Rule
    public final OverridePropertyRule ruleSysPropHecatonLocalAddress = clear("hecaton.local.localAddress");

    private static final int BASE_PORT = 7777;

    @Before
    @After
    public void killAllHecatonInstances() {
        HecatonInstanceFactory.terminateAll();
    }

    @Test
    public void tcp_whenDifferentGroups_thenDifferentClustersAreFormed() {
        whenDifferentGroups_thenDifferentClustersAreFormed(true);
    }

    @Test
    public void multicast_whenDifferentGroups_thenDifferentClustersAreFormed() {
        whenDifferentGroups_thenDifferentClustersAreFormed(false);
    }

    private void whenDifferentGroups_thenDifferentClustersAreFormed(boolean tcp) {
        Config config1 = new Config();
        config1.setClusterName("group1");

        Config config2 = new Config();
        config2.setClusterName("group2");

        if (tcp) {
            enableTcp(config1);
            enableTcp(config2);
        }

        HecatonInstance hz1 = Hecaton.newHecatonInstance(config1);
        HecatonInstance hz2 = Hecaton.newHecatonInstance(config2);

        assertTrue(hz1.getLifecycleService().isRunning());
        assertClusterSize(1, hz1);

        assertTrue(hz2.getLifecycleService().isRunning());
        assertClusterSize(1, hz2);
    }

    private void enableTcp(Config config) {
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        // Use a base port to these nodes can find each other
        // for the case that they are not started from default port (5701)
        final int basePort = TestUtil.getAvailablePort(BASE_PORT);
        config.getNetworkConfig().setPort(basePort);
    }
}
