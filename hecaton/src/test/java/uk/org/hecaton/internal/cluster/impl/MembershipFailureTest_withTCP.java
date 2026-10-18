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
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.TcpIpConfig;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.TestHecatonInstanceFactory.FirewallingNodeContext;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class MembershipFailureTest_withTCP extends MembershipFailureTest {

    @Before
    @Override
    public void init() {
        factory = createHecatonInstanceFactory()
                .withNodeContextDelegator(FirewallingNodeContext::new);
    }

    @Override
    protected Config getConfig() {
        return new Config();
    }

    @Override
    protected Config initConfig(Config config) {
        config = super.initConfig(config);
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "1");
        config.setProperty(ClusterProperty.ASYNC_JOIN_STRATEGY_ENABLED.getName(), "false");

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);

        TcpIpConfig tcpIpConfig = join.getTcpIpConfig().setEnabled(true).clear();
        for (int i = 0; i < 10; i++) {
            int port = 5701 + i;
            tcpIpConfig.addMember("127.0.0.1:" + port);
        }

        return config;
    }
}
