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

package uk.org.hecaton.internal.networking.nio;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.RestServerEndpointConfig;
import uk.org.hecaton.config.ServerSocketEndpointConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.properties.ClusterProperty;
import org.junit.After;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;

public abstract class AbstractAdvancedNetworkIntegrationTest {

    protected static final int MEMBER_PORT = 11000;
    protected static final int CLIENT_PORT = MEMBER_PORT + 1;
    protected static final int WAN1_PORT = MEMBER_PORT + 2;
    protected static final int WAN2_PORT = MEMBER_PORT + 3;
    protected static final int REST_PORT = MEMBER_PORT + 4;
    protected static final int MEMCACHE_PORT = MEMBER_PORT + 5;
    protected static final int NOT_OPENED_PORT = MEMBER_PORT - 1;

    protected final Set<HecatonInstance> instances
            = ConcurrentHashMap.newKeySet();

    @After
    public void tearDown() {
        for (HecatonInstance hz : instances) {
            hz.getLifecycleService().terminate();
        }
    }

    protected HecatonInstance newHecatonInstance(Config config) {
        HecatonInstance hz = Hecaton.newHecatonInstance(config);
        instances.add(hz);
        return hz;
    }

    protected Config createCompleteMultiSocketConfig() {
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig().setEnabled(true)
              .setMemberEndpointConfig(createServerSocketConfig(MEMBER_PORT))
              .setClientEndpointConfig(createServerSocketConfig(CLIENT_PORT))
              .addWanEndpointConfig(createServerSocketConfig(WAN1_PORT, "WAN1"))
              .addWanEndpointConfig(createServerSocketConfig(WAN2_PORT, "WAN2"))
              .setRestEndpointConfig(createRestServerSocketConfig(REST_PORT, "REST"))
              .setMemcacheEndpointConfig(createServerSocketConfig(MEMCACHE_PORT));
        return config;
    }

    protected ServerSocketEndpointConfig createServerSocketConfig(int port) {
        return createServerSocketConfig(port, null);
    }

    protected ServerSocketEndpointConfig createServerSocketConfig(int port, String name) {
        ServerSocketEndpointConfig serverSocketConfig = new ServerSocketEndpointConfig();
        serverSocketConfig.setPort(port);
        serverSocketConfig.getInterfaces().addInterface("127.0.0.1");
        if (name != null) {
            serverSocketConfig.setName(name);
        }
        return serverSocketConfig;
    }

    protected RestServerEndpointConfig createRestServerSocketConfig(int port, String name) {
        RestServerEndpointConfig serverSocketConfig = new RestServerEndpointConfig();
        serverSocketConfig.setPort(port);
        serverSocketConfig.getInterfaces().addInterface("127.0.0.1");
        if (name != null) {
            serverSocketConfig.setName(name);
        }
        serverSocketConfig.enableAllGroups();
        return serverSocketConfig;
    }

    void configureTcpIpConfig(Config config) {
        JoinConfig join = config.getAdvancedNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1:" + NOT_OPENED_PORT).setEnabled(true);
    }

    Config prepareJoinConfigForSecondMember(int port) {
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig().setEnabled(true);
        JoinConfig join = config.getAdvancedNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1:" + port).setEnabled(true);
        config.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        return config;
    }

}
