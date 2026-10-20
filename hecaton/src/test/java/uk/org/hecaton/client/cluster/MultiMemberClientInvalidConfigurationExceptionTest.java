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

package uk.org.hecaton.client.cluster;

import uk.org.hecaton.internal.util.HecatonFeature;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiMemberClientInvalidConfigurationExceptionTest extends ClientTestSupport {

    private TestHecatonFactory factory;
    private static final String EXPECTED_MSG = HecatonFeature.MULTI_MEMBER_ROUTING.notAvailableMessage();

    @Before
    public void before() {
        factory = new TestHecatonFactory();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testMultiMemberClientThrowsInvalidConfigurationException() {
        // create a multi member client config
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.MULTI_MEMBER);

        // Check that we get an UnsupportedOperationException when trying to create client
        // with MULTI_MEMBER routing enabled and no member group defined
        assertThatThrownBy(() -> factory.newHecatonClient(clientConfig))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessage(EXPECTED_MSG);
    }

}
