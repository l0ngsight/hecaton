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

package uk.org.hecaton.spring;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.LoadBalancer;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.test.CustomLoadBalancer;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.test.annotation.QuickTest;

import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;


@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"customLoadBalancer-applicationContext.xml"})
@Category(QuickTest.class)
class TestCustomLoadBalancerContext {

    @Autowired
    private HecatonClientProxy client1;

    @Autowired
    private HecatonClientProxy client2;

    @BeforeAll
    @AfterAll
    public static void start() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Test
    void testCustomLoadBalancer() {
        ClientConfig config1 = client1.getClientConfig();
        LoadBalancer loadBalancer1 = config1.getLoadBalancer();
        assertInstanceOf(CustomLoadBalancer.class, loadBalancer1);
        assertEquals("default-name", ((CustomLoadBalancer) loadBalancer1).getName());

        ClientConfig config2 = client2.getClientConfig();
        LoadBalancer loadBalancer2 = config2.getLoadBalancer();
        assertInstanceOf(CustomLoadBalancer.class, loadBalancer2);
        assertEquals("custom-balancer-name", ((CustomLoadBalancer) loadBalancer2).getName());
    }
}
