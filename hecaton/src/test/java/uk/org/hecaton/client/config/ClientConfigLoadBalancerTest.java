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

package uk.org.hecaton.client.config;

import uk.org.hecaton.client.LoadBalancer;
import uk.org.hecaton.client.impl.clientside.ClientTestUtil;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.test.CustomLoadBalancer;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.client.util.RandomLB;
import uk.org.hecaton.client.util.RoundRobinLB;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientConfigLoadBalancerTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void shouldCreateRoundRobinLoadBalancerWhenNoConfigProvided() {
        hecatonFactory.newHecatonInstance();

        HecatonInstance instance = hecatonFactory.newHecatonClient(new ClientConfig());
        HecatonClientInstanceImpl client = ClientTestUtil.getHecatonClientInstanceImpl(instance);

        LoadBalancer actual = client.getLoadBalancer();
        assertTrue(actual instanceof RoundRobinLB);
    }

    @Test
    public void shouldUseConfigClassLoaderInstanceWhenClassNameNotSpecified() {
        hecatonFactory.newHecatonInstance();

        LoadBalancer loadBalancer = new RandomLB();

        ClientConfig config = new ClientConfig();
        config.setLoadBalancer(loadBalancer);

        HecatonInstance instance = hecatonFactory.newHecatonClient(config);
        HecatonClientInstanceImpl client = ClientTestUtil.getHecatonClientInstanceImpl(instance);

        LoadBalancer actual = client.getLoadBalancer();
        assertSame(loadBalancer, actual);
    }

    @Test
    public void shouldCreateCustomLoadBalancerWhenConfigInstanceNotProvidedAndClassNameSpecified() {
        hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLoadBalancerClassName("uk.org.hecaton.client.test.CustomLoadBalancer");

        HecatonInstance instance = hecatonFactory.newHecatonClient(clientConfig);
        HecatonClientInstanceImpl client = ClientTestUtil.getHecatonClientInstanceImpl(instance);

        LoadBalancer actual = client.getLoadBalancer();
        assertTrue(actual instanceof CustomLoadBalancer);
    }
}
