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

package uk.org.hecaton.internal.server.tcp;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.NetworkConfig;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static uk.org.hecaton.instance.EndpointQualifier.MEMBER;
import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TcpServerContextTest extends HecatonTestSupport {

    private NetworkConfig networkConfig;
    private TcpServerContext serverContext;

    @Before
    public void setUp() {
        Node mockNode = mock(Node.class);
        NodeEngine mockNodeEngine = mock(NodeEngine.class);
        Config config = new Config();
        HecatonProperties properties = new HecatonProperties(config);
        networkConfig = config.getNetworkConfig();
        when(mockNode.getConfig()).thenReturn(config);
        when(mockNode.getProperties()).thenReturn(properties);
        serverContext = new TcpServerContext(mockNode, mockNodeEngine);
    }

    @Test
    public void testGetOutboundPorts_zeroTakesPrecedenceInRange() {
        networkConfig.addOutboundPortDefinition("0-100");
        Collection<Integer> outboundPorts = serverContext.getOutboundPorts(MEMBER);
        assertEquals(0, outboundPorts.size());
    }

    @Test
    public void testGetOutboundPorts_zeroTakesPrecedenceInCSV() {
        networkConfig.addOutboundPortDefinition("5701, 0, 63");
        Collection<Integer> outboundPorts = serverContext.getOutboundPorts(MEMBER);
        assertEquals(0, outboundPorts.size());
    }

    @Test
    public void testGetOutboundPorts_acceptsZero() {
        networkConfig.addOutboundPortDefinition("0");
        Collection<Integer> outboundPorts = serverContext.getOutboundPorts(MEMBER);
        assertEquals(0, outboundPorts.size());
    }

    @Test
    public void testGetOutboundPorts_acceptsWildcard() {
        networkConfig.addOutboundPortDefinition("*");
        Collection<Integer> outboundPorts = serverContext.getOutboundPorts(MEMBER);
        assertEquals(0, outboundPorts.size());
    }

    @Test
    public void testGetOutboundPorts_returnsEmptyCollectionByDefault() {
        Collection<Integer> outboundPorts = serverContext.getOutboundPorts(MEMBER);
        assertEquals(0, outboundPorts.size());
    }

    @Test
    public void testGetOutboundPorts_acceptsRange() {
        networkConfig.addOutboundPortDefinition("29000-29001");
        Collection<Integer> outboundPorts = serverContext.getOutboundPorts(MEMBER);

        assertThat(outboundPorts).hasSize(2);
        assertThat(outboundPorts).containsExactlyInAnyOrder(29000, 29001);
    }

    @Test
    public void testGetOutboundPorts_acceptsSpaceAfterComma() {
        networkConfig.addOutboundPortDefinition("29000, 29001");
        Collection<Integer> outboundPorts = serverContext.getOutboundPorts(MEMBER);

        assertThat(outboundPorts).hasSize(2);
        assertThat(outboundPorts).containsExactlyInAnyOrder(29000, 29001);
    }

    @Test
    public void testGetOutboundPorts_acceptsSpaceAsASeparator() {
        networkConfig.addOutboundPortDefinition("29000 29001");
        Collection<Integer> outboundPorts = serverContext.getOutboundPorts(MEMBER);

        assertThat(outboundPorts).hasSize(2);
        assertThat(outboundPorts).containsExactlyInAnyOrder(29000, 29001);
    }

    @Test
    public void testGetOutboundPorts_acceptsSemicolonAsASeparator() {
        networkConfig.addOutboundPortDefinition("29000;29001");
        Collection<Integer> outboundPorts = serverContext.getOutboundPorts(MEMBER);

        assertThat(outboundPorts).hasSize(2);
        assertThat(outboundPorts).containsExactlyInAnyOrder(29000, 29001);
    }
}
