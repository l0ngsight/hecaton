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
package uk.org.hecaton.internal.cluster.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.TcpIpConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.OverridePropertyRule;
import static uk.org.hecaton.spi.properties.ClusterProperty.ASYNC_JOIN_STRATEGY_ENABLED;
import static uk.org.hecaton.test.OverridePropertyRule.set;
import static uk.org.hecaton.test.TestEnvironment.HECATON_TEST_USE_NETWORK;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import java.util.List;

/**
 * Make sure there are no delays when joining the cluster
 *
 * @author lprimak
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class ClusterJoinDelayTest extends HecatonTestSupport {
    @Rule
    public final OverridePropertyRule overridePropertyRule = set(HECATON_TEST_USE_NETWORK, "true");
    @Rule
    public final OverridePropertyRule overridePropertyRule2 = set(ASYNC_JOIN_STRATEGY_ENABLED.getName(), "true");

    private TestHecatonInstanceFactory fact;
    private final int numInstances = 3;

    @Before
    public void beforeRun() {
        fact = createHecatonInstanceFactory(numInstances);
    }

    @After
    public void afterRun() {
        fact.shutdownAll();
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();
        // make sure the wait is longer than the tested-for delay.
        // here we make sure that the newHecatonInstance() call returns w/o blocking
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "120");
        config.setProperty(ClusterProperty.MAX_WAIT_SECONDS_BEFORE_JOIN.getName(), "480");
        TcpIpConfig tcpIpConfig = new TcpIpConfig();
        tcpIpConfig.setEnabled(true).addMember("localhost");
        config.getNetworkConfig().setPublicAddress("localhost").getJoin().setTcpIpConfig(tcpIpConfig);
        return config;
    }

    @Test
    public void noBlockingBeforeJoin() {
        HecatonInstance hz1 = fact.newHecatonInstance(getConfig());
        HecatonInstance hz2 = fact.newHecatonInstance(getConfig());
        HecatonInstance hz3 = fact.newHecatonInstance(getConfig());
        assertClusterSizeEventually(numInstances, List.of(hz1, hz2, hz3), 30);
    }
}
