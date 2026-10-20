/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.internal.nio.ssl;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.AdvancedNetworkConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.SSLConfig;
import uk.org.hecaton.config.ServerSocketEndpointConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * TLS between members and between clients and members (F1).
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class TlsTest extends HecatonTestSupport {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private TestCertificates ca;
    private Path trustStore;
    private Path memberKeyStore;

    @Before
    public void setUp() throws Exception {
        ca = new TestCertificates(folder.getRoot().toPath(), "test-ca");
        trustStore = ca.trustStore();
        memberKeyStore = ca.keyStore("member", "127.0.0.1", "localhost");
    }

    @After
    public void tearDown() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Test
    public void membersFormAClusterAndClientsConnectOverTls() {
        HecatonInstance member1 = member(ssl(memberKeyStore, trustStore));
        HecatonInstance member2 = member(ssl(memberKeyStore, trustStore));
        assertClusterSizeEventually(2, member1, member2);

        HecatonInstance client = client(ssl(null, trustStore), member1);
        client.getMap("m").put("k", "v");
        assertThat(member2.getMap("m").get("k")).isEqualTo("v");
    }

    @Test
    public void largeValuesCrossTls() {
        HecatonInstance member = member(ssl(memberKeyStore, trustStore));
        HecatonInstance client = client(ssl(null, trustStore), member);
        byte[] value = new byte[3 * 1024 * 1024];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) i;
        }
        client.getMap("m").put("k", value);
        assertThat((byte[]) client.getMap("m").get("k")).isEqualTo(value);
    }

    @Test
    public void plainClientCannotConnectToTlsMember() {
        HecatonInstance member = member(ssl(memberKeyStore, trustStore));
        assertThatThrownBy(() -> client(null, member)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void clientThatDoesNotTrustTheMemberCannotConnect() throws Exception {
        HecatonInstance member = member(ssl(memberKeyStore, trustStore));
        Path otherTrust = new TestCertificates(folder.newFolder().toPath(), "other-ca").trustStore();
        assertThatThrownBy(() -> client(ssl(null, otherTrust), member)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void mutualAuthenticationRequiresAClientCertificate() throws Exception {
        SSLConfig memberSsl = ssl(memberKeyStore, trustStore).setProperty("mutualAuthentication", "REQUIRED");
        HecatonInstance member = member(memberSsl);

        assertThatThrownBy(() -> client(ssl(null, trustStore), member)).isInstanceOf(IllegalStateException.class);

        HecatonInstance client = client(ssl(ca.keyStore("client"), trustStore), member);
        client.getMap("m").put("k", "v");
        assertThat(member.getMap("m").get("k")).isEqualTo("v");
    }

    @Test
    public void identityValidationChecksTheMembersCertificate() throws Exception {
        HecatonInstance matching = member(ssl(memberKeyStore, trustStore));
        client(ssl(null, trustStore).setProperty("validateIdentity", "true"), matching).getMap("m").put("k", "v");

        HecatonInstance other = member(ssl(ca.keyStore("elsewhere", "10.255.255.1"), trustStore));
        assertThatThrownBy(() -> client(ssl(null, trustStore).setProperty("validateIdentity", "true"), other))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void restOverTls() throws Exception {
        Config config = memberConfig(ssl(memberKeyStore, trustStore));
        config.getNetworkConfig().getRestApiConfig().setEnabled(true).enableAllGroups();
        HecatonInstance member = Hecaton.newHecatonInstance(config);
        int port = member.getCluster().getLocalMember().getAddress().getPort();

        KeyStore trusted = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(trustStore)) {
            trusted.load(in, TestCertificates.PASSWORD.toCharArray());
        }
        TrustManagerFactory trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagers.init(trusted);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trustManagers.getTrustManagers(), null);

        HttpsURLConnection connection = (HttpsURLConnection) URI.create(
                "https://127.0.0.1:" + port + "/hecaton/health/ready").toURL().openConnection();
        connection.setSSLSocketFactory(context.getSocketFactory());
        assertThat(connection.getResponseCode()).isEqualTo(200);
    }

    @Test
    public void advancedNetworkingWithTlsOnTheClientEndpointOnly() {
        HecatonInstance member1 = Hecaton.newHecatonInstance(advancedConfig());
        HecatonInstance member2 = Hecaton.newHecatonInstance(advancedConfig());
        assertClusterSizeEventually(2, member1, member2);

        ClientConfig config = new ClientConfig().setClusterName("tls");
        config.getNetworkConfig().addAddress("127.0.0.1:5821").setSSLConfig(ssl(null, trustStore));
        HecatonInstance client = HecatonClient.newHecatonClient(config);
        client.getMap("m").put("k", "v");
        assertThat(member2.getMap("m").get("k")).isEqualTo("v");
    }

    private Config advancedConfig() {
        Config config = smallInstanceConfigWithoutJetAndMetrics().setClusterName("tls");
        AdvancedNetworkConfig network = config.getAdvancedNetworkConfig().setEnabled(true);
        network.setMemberEndpointConfig(new ServerSocketEndpointConfig().setPort(5811));
        network.setClientEndpointConfig(new ServerSocketEndpointConfig().setPort(5821)
                .setSSLConfig(ssl(memberKeyStore, trustStore)));
        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1:5811").addMember("127.0.0.1:5812");
        return config;
    }

    private SSLConfig ssl(Path keyStore, Path trustStore) {
        Properties properties = new Properties();
        if (keyStore != null) {
            properties.setProperty("keyStore", keyStore.toString());
            properties.setProperty("keyStorePassword", TestCertificates.PASSWORD);
            properties.setProperty("keyStoreType", "PKCS12");
        }
        properties.setProperty("trustStore", trustStore.toString());
        properties.setProperty("trustStorePassword", TestCertificates.PASSWORD);
        properties.setProperty("trustStoreType", "PKCS12");
        return new SSLConfig().setEnabled(true).setProperties(properties);
    }

    private static HecatonInstance member(SSLConfig ssl) {
        return Hecaton.newHecatonInstance(memberConfig(ssl));
    }

    private static Config memberConfig(SSLConfig ssl) {
        Config config = smallInstanceConfigWithoutJetAndMetrics().setClusterName("tls");
        config.getNetworkConfig().setPort(5801).setSSLConfig(ssl);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        return config;
    }

    private static HecatonInstance client(SSLConfig ssl, HecatonInstance member) {
        ClientConfig config = new ClientConfig().setClusterName("tls");
        config.getNetworkConfig().addAddress("127.0.0.1:" + member.getCluster().getLocalMember().getAddress().getPort());
        if (ssl != null) {
            config.getNetworkConfig().setSSLConfig(ssl);
        }
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(5000);
        return HecatonClient.newHecatonClient(config);
    }
}
