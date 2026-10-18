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

package uk.org.hecaton.test;

import uk.org.hecaton.config.AdvancedNetworkConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.NetworkConfig;
import uk.org.hecaton.config.ServerSocketEndpointConfig;
import uk.org.hecaton.config.TcpIpConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.impl.DefaultNodeContext;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.instance.impl.NodeContext;
import uk.org.hecaton.internal.jmx.ManagementService;
import uk.org.hecaton.test.annotation.ParallelJVMTest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static uk.org.hecaton.config.Config.DEFAULT_CLUSTER_NAME;
import static uk.org.hecaton.instance.EndpointQualifier.CLIENT;
import static uk.org.hecaton.instance.EndpointQualifier.MEMBER;
import static uk.org.hecaton.test.AbstractHecatonClassRunner.getTestMethodName;
import static uk.org.hecaton.test.Accessors.getAddress;

/**
 * Per test-method factory for Hecaton members. It sets existing members host:port from given cluster to TCP join
 * configuration of other members. The instances are kept per test method which allows to terminate them in
 * {@link org.junit.After} methods (see {@link #terminateAll()}). The factory methods also sets custom cluster name which prevents
 * accidental joins (e.g. dangling members).
 * <p>
 * <b>Tests using this factory should not be annotated with {@link ParallelJVMTest} category to avoid runs in multiple JVMs.</b>
 * <p>
 * Example:
 *
 * <pre>
 * &#64;RunWith(HecatonParallelClassRunner.class)
 * &#64;Category(QuickTest.class)
 * public class Test {
 *
 *     private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();
 *
 *     &#64;After
 *     public void after() {
 *         factory.terminateAll();
 *     }
 *
 *     &#64;Test
 *     public void test1() {
 *         Config config = new Config();
 *         HecatonInstance h1 = factory.newHecatonInstance(config);
 *         HecatonInstance h2 = factory.newHecatonInstance(config);
 *         // ...
 *     }
 *
 *     &#64;Test
 *     public void test2() {
 *         Config config = new Config();
 *         HecatonInstance h1 = factory.newHecatonInstance(config);
 *         HecatonInstance h2 = factory.newHecatonInstance(config);
 *         // ...
 *     }
 * }
 * </pre>
 */
public class TestAwareInstanceFactory {

    private static final AtomicInteger PORT = new AtomicInteger(5000);

    protected final Map<String, List<HecatonInstance>> perMethodMembers = new ConcurrentHashMap<>();

    /**
     * Calls {@link #newHecatonInstance(Config, NodeContext)} using the
     * {@link DefaultNodeContext}.
     */
    public HecatonInstance newHecatonInstance(Config config) {
        return newHecatonInstance(config, new DefaultNodeContext());
    }

    /**
     * Creates new member instance with TCP join configured. Uses
     * {@link uk.org.hecaton.test.AbstractHecatonClassRunner#getTestMethodName()}
     * as the cluster name if it's not changed already.
     */
    public HecatonInstance newHecatonInstance(Config config, NodeContext nodeCtx) {
        if (config == null) {
            config = new Config();
        }
        if (DEFAULT_CLUSTER_NAME.equals(config.getClusterName())) {
            config.setClusterName(getTestMethodName());
        }
        List<HecatonInstance> members = getOrInitInstances(perMethodMembers);

        // Prepare Unified Networking (legacy)
        NetworkConfig unifiedNetworkingConfig = config.getNetworkConfig();
        unifiedNetworkingConfig.setPort(PORT.getAndIncrement());
        JoinConfig unifiedJoinConfig = unifiedNetworkingConfig.getJoin();
        unifiedJoinConfig.getMulticastConfig().setEnabled(false);
        TcpIpConfig unifiedTcpIpConfig = unifiedJoinConfig.getTcpIpConfig().setEnabled(true);
        for (HecatonInstance member : members) {
            unifiedTcpIpConfig.addMember("127.0.0.1:" + getPort(member, MEMBER));
        }

        // Prepare Advanced Networking - Will be disabled by default but properly configured if needed
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        ServerSocketEndpointConfig memberEndpointConfig
                = (ServerSocketEndpointConfig) advancedNetworkConfig.getEndpointConfigs().get(MEMBER);
        memberEndpointConfig.setPort(PORT.getAndIncrement());
        ServerSocketEndpointConfig clientEndpointConfig = (ServerSocketEndpointConfig) advancedNetworkConfig
                .getEndpointConfigs().get(CLIENT);
        if (clientEndpointConfig != null) {
            clientEndpointConfig.setPort(PORT.getAndIncrement());
        }
        JoinConfig advancedJoinConfig = advancedNetworkConfig.getJoin();
        advancedJoinConfig.getMulticastConfig().setEnabled(false);
        TcpIpConfig advancedTcpIpConfig = advancedJoinConfig.getTcpIpConfig().setEnabled(true);
        for (HecatonInstance member : members) {
            advancedTcpIpConfig.addMember("127.0.0.1:" + getPort(member, MEMBER));
        }

        HecatonInstance hz = HecatonInstanceFactory.newHecatonInstance(
                config, config.getInstanceName(), nodeCtx);
        members.add(hz);
        int nextPort = Math.max(getPort(hz, MEMBER), getPort(hz, CLIENT)) + 1;
        int current;
        while (nextPort > (current = PORT.get())) {
            PORT.compareAndSet(current, nextPort);
        }
        return hz;
    }

    /**
     * Terminates all member instances created by this factory for current test method name.
     */
    public void terminateAll() {
        shutdownInstances(perMethodMembers.remove(getTestMethodName()));
    }

    protected void shutdownInstances(List<HecatonInstance> listToRemove) {
        if (listToRemove != null) {
            for (HecatonInstance hz : listToRemove) {
                try {
                    ManagementService.shutdown(hz.getName());
                    hz.getLifecycleService().terminate();
                } catch (Exception e) {
                    // just log it - it can be already down for instance
                    Logger.getLogger(getClass().getName())
                            .log(Level.FINEST, "Shutdown failed. The instance might have already been terminated.", e);
                }
            }
        }
    }

    protected List<HecatonInstance> getOrInitInstances(Map<String, List<HecatonInstance>> map) {
        String methodName = getTestMethodName();
        List<HecatonInstance> list = map.computeIfAbsent(methodName, k -> new CopyOnWriteArrayList<>());
        return list;
    }

    protected static int getPort(HecatonInstance hz, EndpointQualifier qualifier) {
        return getAddress(hz, qualifier).getPort();
    }
}
