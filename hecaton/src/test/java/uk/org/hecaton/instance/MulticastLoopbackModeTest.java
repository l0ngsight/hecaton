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

package uk.org.hecaton.instance;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MulticastConfig;
import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

/**
 * Test the multicast loopback mode when there is no other
 * network interface than 127.0.0.1.
 *
 * @author St&amp;eacute;phane Galland &lt;galland{@literal @}arakhne.org&gt;
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class MulticastLoopbackModeTest extends HecatonTestSupport {

    private HecatonInstance hz1;
    private HecatonInstance hz2;

    @Before
    public void setUpTests() {
        assumeFalse("This test can be processed only if your host has no configured network interface.",
                hasConfiguredNetworkInterface());
    }

    @After
    public void tearDownTests() {
        HecatonInstanceFactory.terminateAll();
    }

    private void createTestEnvironment(boolean loopbackMode) {
        Config config = new Config();
        config.setProperty("hecaton.local.localAddress", "127.0.0.1");
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        multicastConfig.setEnabled(true);
        multicastConfig.setLoopbackModeEnabled(loopbackMode);

        hz1 = HecatonInstanceFactory.newHecatonInstance(config);
        assertNotNull("Cannot create the first HecatonInstance", hz1);

        hz2 = HecatonInstanceFactory.newHecatonInstance(config);
        assertNotNull("Cannot create the second HecatonInstance", hz2);
    }

    @Test
    public void testEnabledMode() {
        createTestEnvironment(true);

        assertClusterSize(2, hz1, hz2);

        Cluster cluster1 = hz1.getCluster();
        Cluster cluster2 = hz2.getCluster();
        assertTrue("Members list " + cluster1.getMembers() + " should contain " + cluster2.getLocalMember(),
                cluster1.getMembers().contains(cluster2.getLocalMember()));
        assertTrue("Members list " + cluster2.getMembers() + " should contain " + cluster1.getLocalMember(),
                cluster2.getMembers().contains(cluster1.getLocalMember()));
    }

    @Test
    public void testDisabledMode() {
        createTestEnvironment(false);

        assertClusterSize(1, hz1);
        assertClusterSize(1, hz2);
    }

    /**
     * Replies if a network interface was properly configured.
     *
     * @return <code>true</code> if there is at least one configured interface;
     * <code>false</code> otherwise.
     */
    private static boolean hasConfiguredNetworkInterface() {
        try {
            Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
            while (e.hasMoreElements()) {
                NetworkInterface i = e.nextElement();
                Enumeration<InetAddress> as = i.getInetAddresses();
                while (as.hasMoreElements()) {
                    InetAddress a = as.nextElement();
                    if (a instanceof Inet4Address && !a.isLoopbackAddress() && !a.isMulticastAddress()) {
                        return true;
                    }
                }
            }
        } catch (Exception ignored) {
            // silently cast the exceptions
        }
        return false;
    }
}
