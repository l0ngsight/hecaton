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
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonTestSupport;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractJoinTest extends HecatonTestSupport {

    protected void testJoin(Config config) {
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "1");
        config.setProperty(ClusterProperty.ASYNC_JOIN_STRATEGY_ENABLED.getName(), "false");

        HecatonInstance h1 = Hecaton.newHecatonInstance(config);
        assertClusterSize(1, h1);

        HecatonInstance h2 = Hecaton.newHecatonInstance(config);
        assertClusterSize(2, h1, h2);

        h1.shutdown();
        h1 = Hecaton.newHecatonInstance(config);
        // when h1 is returned, it's guaranteed that it should see 2 members
        assertClusterSize(2, h1);
        assertClusterSize(2, h2);
    }

    protected void testJoinEventually(Config config) {
        HecatonInstance h1 = Hecaton.newHecatonInstance(config);
        assertClusterSize(1, h1);

        HecatonInstance h2 = Hecaton.newHecatonInstance(config);
        assertClusterSize(2, h1, h2);

        h1.shutdown();
        h1 = Hecaton.newHecatonInstance(config);
        // when h1 is returned, it's guaranteed that it should see 2 members
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);
    }

    protected void testJoin_With_DifferentBuildNumber(Config config) {
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");

        String buildNumberProp = "hecaton.build";
        System.setProperty(buildNumberProp, "1");
        try {
            HecatonInstance h1 = Hecaton.newHecatonInstance(config);

            System.setProperty(buildNumberProp, "2");
            HecatonInstance h2 = Hecaton.newHecatonInstance(config);

            assertClusterSize(2, h1, h2);
        } finally {
            System.clearProperty(buildNumberProp);
        }
    }

    /**
     * Checks if a HecatonInstance created with config2, can be added to a HecatonInstance created with config 1.
     * <p>
     * This method expects that an IllegalStateException is thrown when the second HecatonInstance is created, and
     * it doesn't join the cluster but gets killed instead.
     */
    protected void assertIncompatible(Config config1, Config config2) {
        HecatonInstance hz1 = Hecaton.newHecatonInstance(config1);

        try {
            Hecaton.newHecatonInstance(config2);
            fail();
        } catch (IllegalStateException e) {

        }

        assertTrue(hz1.getLifecycleService().isRunning());
        assertClusterSize(1, hz1);
    }

    protected void assertIndependentClusters(Config config1, Config config2) {
        HecatonInstance hz1 = Hecaton.newHecatonInstance(config1);
        HecatonInstance hz2 = Hecaton.newHecatonInstance(config2);

        assertTrue(hz1.getLifecycleService().isRunning());
        assertClusterSize(1, hz1);

        assertTrue(hz2.getLifecycleService().isRunning());
        assertClusterSize(1, hz2);
    }

    protected static void assertIndependentClustersAndDoNotMergedEventually(Config config1, Config config2, int durationSeconds) {
        final HecatonInstance hz1 = Hecaton.newHecatonInstance(config1);
        final HecatonInstance hz2 = Hecaton.newHecatonInstance(config2);

        assertTrue(hz1.getLifecycleService().isRunning());
        assertClusterSize(1, hz1);

        assertTrue(hz2.getLifecycleService().isRunning());
        assertClusterSize(1, hz2);

        assertTrueAllTheTime(() -> {
            assertClusterSize(1, hz1);
            assertClusterSize(1, hz2);
        }, durationSeconds);
    }

    protected static InetAddress pickLocalInetAddress() throws IOException {
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            NetworkInterface ni = networkInterfaces.nextElement();
            if (!ni.isUp() || ni.isVirtual() || ni.isLoopback() || !ni.supportsMulticast()) {
                continue;
            }
            Enumeration<InetAddress> e = ni.getInetAddresses();
            while (e.hasMoreElements()) {
                InetAddress inetAddress = e.nextElement();
                if (inetAddress instanceof Inet6Address) {
                    continue;
                }
                return inetAddress;
            }
        }
        return InetAddress.getLocalHost();
    }
}
