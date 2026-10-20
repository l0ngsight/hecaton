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
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.RoutingStrategy;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.config.KubernetesConfig;
import uk.org.hecaton.config.SocketInterceptorConfig;
import uk.org.hecaton.core.Hecaton;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"clientNetworkConfig-applicationContext.xml"})
class TestClientNetworkConfig {

    @Autowired
    private HecatonClientProxy client;

    @BeforeAll
    @AfterAll
    public static void start() {
        System.setProperty("test.keyStore", "private.jks");
        System.setProperty("test.trustStore", "trust.jks");

        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Test
    void smokeMember() {
        int memberCountInConfigurationXml = 10;
        ClientConfig config = client.getClientConfig();
        assertEquals(memberCountInConfigurationXml, config.getNetworkConfig().getAddresses().size());
    }

    @Test
    void smokeSocketOptions() {
        int bufferSizeInConfigurationXml = 32;
        ClientConfig config = client.getClientConfig();
        assertEquals(bufferSizeInConfigurationXml, config.getNetworkConfig().getSocketOptions().getBufferSize());
    }

    @Test
    void smokeSocketInterceptor() {
        ClientConfig config = client.getClientConfig();
        SocketInterceptorConfig socketInterceptorConfig = config.getNetworkConfig().getSocketInterceptorConfig();
        assertFalse(socketInterceptorConfig.isEnabled());
        assertEquals(DummySocketInterceptor.class.getName(), socketInterceptorConfig.getClassName());
    }

    @Test
    void smokeSSLConfig() {
        ClientConfig config = client.getClientConfig();
        assertEquals("uk.org.hecaton.nio.ssl.BasicSSLContextFactory",
                config.getNetworkConfig().getSSLConfig().getFactoryClassName());
    }

    @Test
    void smokeClusterRoutingConfig() {
        ClientConfig config = client.getClientConfig();
        assertEquals(RoutingMode.ALL_MEMBERS, config.getNetworkConfig().getClusterRoutingConfig().getRoutingMode());
        assertEquals(RoutingStrategy.PARTITION_GROUPS,
                config.getNetworkConfig().getClusterRoutingConfig().getRoutingStrategy());
    }

    @Test
    void smokeKubernetesConfig() {
        KubernetesConfig kubernetes = client.getClientConfig().getNetworkConfig().getKubernetesConfig();
        assertFalse(kubernetes.isEnabled());
        assertEquals("MY-KUBERNETES-NAMESPACE", kubernetes.getProperty("namespace"));
        assertEquals("MY-SERVICE-NAME", kubernetes.getProperty("service-name"));
        assertEquals("MY-SERVICE-LABEL-NAME", kubernetes.getProperty("service-label-name"));
        assertEquals("MY-SERVICE-LABEL-VALUE", kubernetes.getProperty("service-label-value"));
    }

    @Test
    void smokeOutboundPorts() {
        Collection<String> allowedPorts = client.getClientConfig().getNetworkConfig().getOutboundPortDefinitions();
        assertEquals(2, allowedPorts.size());
        assertTrue(allowedPorts.contains("34600"));
        assertTrue(allowedPorts.contains("34700-34710"));
    }
}
