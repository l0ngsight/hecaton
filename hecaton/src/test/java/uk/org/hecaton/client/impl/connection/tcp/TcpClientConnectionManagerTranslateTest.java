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

package uk.org.hecaton.client.impl.connection.tcp;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.HecatonClientUtil;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.connection.AddressProvider;
import uk.org.hecaton.client.impl.connection.Addresses;
import uk.org.hecaton.client.impl.management.ClientConnectionProcessListenerRegistry;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.BuildInfoProvider;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.ProtocolType;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.version.MemberVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpClientConnectionManagerTranslateTest extends ClientTestSupport {
    private static final MemberVersion VERSION = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());

    private Address privateAddress;
    private Address publicAddress;

    @Before
    public void setup() throws Exception {
        Hecaton.newHecatonInstance();

        // correct private address
        privateAddress = new Address("127.0.0.1", 5701);
        // incorrect public address
        publicAddress = new Address("192.168.0.1", 5701);
    }

    @After
    public void teardown() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Test(expected = Exception.class)
    public void testTranslateIsUsed() {
        // given
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(1000);
        HecatonInstance client = HecatonClientUtil.newHecatonClient(config, new TestAddressProvider(true));
        TcpClientConnectionManager clientConnectionManager =
                new TcpClientConnectionManager(getHecatonClientInstanceImpl(client));

        // when
        clientConnectionManager.start();

        // then
        // throws exception because it can't connect to the cluster using translated public unreachable address
    }

    @Test
    public void testTranslateIsNotUsedOnGettingExistingConnection() {
        // given
        TestAddressProvider provider = new TestAddressProvider(false);
        HecatonInstance client = HecatonClientUtil.newHecatonClient(new ClientConfig(), provider);
        TcpClientConnectionManager clientConnectionManager =
                new TcpClientConnectionManager(getHecatonClientInstanceImpl(client));

        clientConnectionManager.start();
        clientConnectionManager.reset();

        clientConnectionManager.getOrConnectToAddress(privateAddress, false);
        provider.shouldTranslate = true;

        // when
        Connection connection = clientConnectionManager.getOrConnectToAddress(privateAddress, false);

        // then
        assertNotNull(connection);
    }

    @Test
    public void testTranslateIsUsedWhenMemberHasPublicClientAddress() throws UnknownHostException {
        // given
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED.getName(), "true");

        HecatonInstance client = HecatonClientUtil.newHecatonClient(clientConfig, null);
        TcpClientConnectionManager clientConnectionManager =
                new TcpClientConnectionManager(getHecatonClientInstanceImpl(client));
        clientConnectionManager.start();

        // private member address is unreachable
        Member member = new MemberImpl(new Address("192.168.0.1", 5701), VERSION, false, UUID.randomUUID());
        // public member address is reachable
        member.getAddressMap().put(EndpointQualifier.resolve(ProtocolType.CLIENT, "public"),
                new Address("127.0.0.1", 5701));

        // when
        Connection connection = clientConnectionManager.getOrConnectToMember(member, false);

        // then
        assertNotNull(connection);
    }

    @Test(expected = Exception.class)
    public void testTranslateIsNotUsedWhenPublicIpDisabled() throws UnknownHostException {
        // given
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED.getName(), "false");

        HecatonInstance client = HecatonClientUtil.newHecatonClient(clientConfig, null);
        TcpClientConnectionManager clientConnectionManager =
                new TcpClientConnectionManager(getHecatonClientInstanceImpl(client));
        clientConnectionManager.start();

        // private member address is incorrect
        Member member = new MemberImpl(new Address("192.168.0.1", 5701), VERSION, false);
        // public member address is correct
        member.getAddressMap().put(EndpointQualifier.resolve(ProtocolType.CLIENT, "public"), new Address("127.0.0.1", 5701));

        // when
        clientConnectionManager.getOrConnectToMember(member, false);

        // then
        // throws exception because it can't connect to the incorrect member address
    }

    @Test(expected = Exception.class)
    public void testTranslateFromMemberIsNotUsedWhenAlreadyTranslatedByAddressProvider() throws UnknownHostException {
        // given
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED.getName(), "true");

        TestAddressProvider provider = new TestAddressProvider(false);
        HecatonInstance client = HecatonClientUtil.newHecatonClient(clientConfig, provider);
        TcpClientConnectionManager clientConnectionManager =
                new TcpClientConnectionManager(getHecatonClientInstanceImpl(client));
        clientConnectionManager.start();
        provider.shouldTranslate = true;
        privateAddress = new Address("192.168.0.1", 5702);

        // private member address is correct
        Member member = new MemberImpl(privateAddress, VERSION, false, UUID.randomUUID());
        // public member address is correct
        member.getAddressMap().put(EndpointQualifier.resolve(ProtocolType.CLIENT, "public"),
                new Address("127.0.0.1", 5701));

        // when
        clientConnectionManager.getOrConnectToMember(member, false);

        // then
        // throws exception because it can't connect to the incorrect address
    }

    private class TestAddressProvider implements AddressProvider {

        private boolean shouldTranslate;

        private TestAddressProvider(boolean shouldTranslate) {
            this.shouldTranslate = shouldTranslate;
        }

        @Override
        public Address translate(Address address) {
            if (!shouldTranslate) {
                return address;
            }

            if (address.equals(privateAddress)) {
                return publicAddress;
            }
            return null;
        }

        @Override
        public Address translate(Member member) {
            return member.getAddress();
        }

        @Override
        public Addresses loadAddresses(ClientConnectionProcessListenerRegistry listenerRunner) {
            try {
                return new Addresses(List.of(new Address("127.0.0.1", 5701)));
            } catch (UnknownHostException e) {
                return null;
            }
        }
    }
}
