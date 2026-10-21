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

package uk.org.hecaton.security.impl;

import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.PermissionConfig;
import uk.org.hecaton.config.PermissionConfig.PermissionType;
import uk.org.hecaton.config.SecurityConfig;
import uk.org.hecaton.config.security.KerberosAuthenticationConfig;
import uk.org.hecaton.config.security.KerberosIdentityConfig;
import uk.org.hecaton.config.security.RealmConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.net.ServerSocket;
import java.security.AccessControlException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Kerberos authentication (F2), against an in-process KDC.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class KerberosAuthenticationTest extends HecatonTestSupport {

    private static final String REALM = "EXAMPLE.COM";
    private static final String SERVICE = "hz/localhost@" + REALM;

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    private static SimpleKdcServer kdc;
    private static File serviceKeytab;
    private static File aliceKeytab;
    private static File bobKeytab;

    @BeforeClass
    public static void startKdc() throws Exception {
        File dir = folder.newFolder("kdc");
        kdc = new SimpleKdcServer();
        kdc.setWorkDir(dir);
        kdc.setKdcHost("localhost");
        kdc.setKdcRealm(REALM);
        kdc.setAllowUdp(false);
        try (ServerSocket socket = new ServerSocket(0)) {
            kdc.setKdcTcpPort(socket.getLocalPort());
        }
        kdc.init();
        kdc.start();
        System.setProperty("java.security.krb5.conf", new File(dir, "krb5.conf").getAbsolutePath());
        serviceKeytab = keytab(SERVICE);
        aliceKeytab = keytab("alice@" + REALM);
        bobKeytab = keytab("bob@" + REALM);
    }

    private static File keytab(String principal) throws Exception {
        kdc.createPrincipal(principal, "pw-" + principal);
        File keytab = new File(folder.getRoot(), principal.replace('/', '_') + ".keytab");
        kdc.exportPrincipal(principal, keytab);
        return keytab;
    }

    @AfterClass
    public static void stopKdc() throws Exception {
        kdc.stop();
        System.clearProperty("java.security.krb5.conf");
    }

    @After
    public void tearDown() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Test
    public void clientsAuthenticateWithKerberosTickets() {
        HecatonInstance member = startMember();

        client(member, "alice@" + REALM, aliceKeytab).getMap("data").put("k", "v");
        HecatonInstance bob = client(member, "bob@" + REALM, bobKeytab);
        assertThat(bob.getMap("data").get("k")).isEqualTo("v");
        assertThatThrownBy(() -> bob.getMap("data").put("k", "x")).isInstanceOf(AccessControlException.class);
    }

    @Test
    public void ticketsForAnotherServiceAreRejected() throws Exception {
        HecatonInstance member = startMember();
        keytab("hz/other@" + REALM);

        ClientConfig config = clientConfig(member);
        config.getSecurityConfig().setKerberosIdentityConfig(new KerberosIdentityConfig()
                .setSpn("hz/other@" + REALM).setPrincipal("alice@" + REALM).setKeytabFile(aliceKeytab.getAbsolutePath()));
        assertThatThrownBy(() -> HecatonClient.newHecatonClient(config)).isInstanceOf(IllegalStateException.class);
    }

    private HecatonInstance startMember() {
        Config config = smallInstanceConfigWithoutJetAndMetrics().setClusterName("kerberos");
        config.getNetworkConfig().setPort(5921);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.setSecurityConfig(new SecurityConfig().setEnabled(true)
                .setClientRealmConfig("kerberos", new RealmConfig().setKerberosAuthenticationConfig(
                        new KerberosAuthenticationConfig().setPrincipal(SERVICE)
                                .setKeytabFile(serviceKeytab.getAbsolutePath())))
                .addClientPermissionConfig(new PermissionConfig(PermissionType.ALL, null, "alice@" + REALM))
                .addClientPermissionConfig(new PermissionConfig(PermissionType.MAP, "data", "*").addAction("create")
                        .addAction("read")));
        return Hecaton.newHecatonInstance(config);
    }

    private static HecatonInstance client(HecatonInstance member, String principal, File keytab) {
        ClientConfig config = clientConfig(member);
        config.getSecurityConfig().setKerberosIdentityConfig(new KerberosIdentityConfig()
                .setSpn(SERVICE).setPrincipal(principal).setKeytabFile(keytab.getAbsolutePath()));
        return HecatonClient.newHecatonClient(config);
    }

    private static ClientConfig clientConfig(HecatonInstance member) {
        ClientConfig config = new ClientConfig().setClusterName("kerberos");
        config.getNetworkConfig().addAddress("127.0.0.1:" + member.getCluster().getLocalMember().getAddress().getPort());
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(3000);
        return config;
    }
}
