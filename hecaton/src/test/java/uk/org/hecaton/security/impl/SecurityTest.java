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

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.OnJoinPermissionOperationName;
import uk.org.hecaton.config.PermissionConfig;
import uk.org.hecaton.config.PermissionConfig.PermissionType;
import uk.org.hecaton.config.SSLConfig;
import uk.org.hecaton.config.SecurityConfig;
import uk.org.hecaton.config.security.RealmConfig;
import uk.org.hecaton.config.security.SimpleAuthenticationConfig;
import uk.org.hecaton.config.security.TlsAuthenticationConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nio.ssl.TestCertificates;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static uk.org.hecaton.internal.util.EmptyStatement.ignore;
import static uk.org.hecaton.test.Accessors.getNode;

/**
 * Authentication (F2) and client permissions (F3).
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class SecurityTest extends HecatonTestSupport {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @After
    public void tearDown() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Test
    public void usersGetThePermissionsOfTheirRoles() {
        Config config = memberConfig();
        config.getSecurityConfig()
                .setClientRealmConfig("users", new RealmConfig().setSimpleAuthenticationConfig(new SimpleAuthenticationConfig()
                        .addUser("alice", "alice-pw", "admins")
                        .addUser("bob", "bob-pw", "readers")))
                .addClientPermissionConfig(new PermissionConfig(PermissionType.ALL, null, "admins"))
                .addClientPermissionConfig(permission(PermissionType.MAP, "data", "readers", "create", "read"));
        HecatonInstance member = Hecaton.newHecatonInstance(config);
        member.getMap("data").put("k", "v");

        IMap<String, String> asAlice = client(member, "alice", "alice-pw").getMap("data");
        asAlice.put("k2", "v2");
        assertThat(asAlice.get("k")).isEqualTo("v");

        HecatonInstance bob = client(member, "bob", "bob-pw");
        assertThat(bob.<String, String>getMap("data").get("k")).isEqualTo("v");
        assertThatThrownBy(() -> bob.getMap("data").put("k", "x")).isInstanceOf(AccessControlException.class);
        assertThatThrownBy(() -> bob.getMap("other").get("k")).isInstanceOf(AccessControlException.class);
    }

    @Test
    public void wrongPasswordOrUnknownUserCannotConnect() {
        Config config = memberConfig();
        config.getSecurityConfig().setClientRealmConfig("users", new RealmConfig()
                .setSimpleAuthenticationConfig(new SimpleAuthenticationConfig().addUser("alice", "alice-pw")));
        HecatonInstance member = Hecaton.newHecatonInstance(config);

        assertThatThrownBy(() -> client(member, "alice", "wrong")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> client(member, "mallory", "alice-pw")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> client(member, null, null)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void denyTakesPrecedenceUnlessGrantsHavePriority() {
        Config config = memberConfig();
        config.getSecurityConfig()
                .setClientRealmConfig("users", new RealmConfig()
                        .setSimpleAuthenticationConfig(new SimpleAuthenticationConfig().addUser("alice", "alice-pw")))
                .addClientPermissionConfig(new PermissionConfig(PermissionType.ALL, null, "*"))
                .addClientPermissionConfig(permission(PermissionType.MAP, "secret", "*", "all").setDeny(true));
        HecatonInstance member = Hecaton.newHecatonInstance(config);

        HecatonInstance alice = client(member, "alice", "alice-pw");
        alice.getMap("public").put("k", "v");
        assertThatThrownBy(() -> alice.getMap("secret").put("k", "v")).isInstanceOf(AccessControlException.class);
    }

    @Test
    public void permissionsCanBeChangedAtRuntime() {
        Config config = memberConfig();
        config.getSecurityConfig()
                .setClientRealmConfig("users", new RealmConfig()
                        .setSimpleAuthenticationConfig(new SimpleAuthenticationConfig().addUser("alice", "alice-pw")))
                .addClientPermissionConfig(permission(PermissionType.MAP, "data", "*", "create", "read"));
        HecatonInstance member = Hecaton.newHecatonInstance(config);
        HecatonInstance alice = client(member, "alice", "alice-pw");
        assertThatThrownBy(() -> alice.getMap("data").put("k", "v")).isInstanceOf(AccessControlException.class);

        member.getConfig().getSecurityConfig().setClientPermissionConfigs(
                Set.of(permission(PermissionType.MAP, "data", "*", "create", "read", "put")));
        alice.getMap("data").put("k", "v");
    }

    @Test
    public void membersMustPresentTheMemberIdentity() {
        HecatonInstance member1 = Hecaton.newHecatonInstance(memberIdentityConfig("member-pw"));
        HecatonInstance member2 = Hecaton.newHecatonInstance(memberIdentityConfig("member-pw"));
        assertClusterSizeEventually(2, member1, member2);

        // a member whose join is refused shuts down
        assertThatThrownBy(() -> Hecaton.newHecatonInstance(memberIdentityConfig("guess")))
                .isInstanceOf(IllegalStateException.class);
        assertClusterSize(2, member1);
    }

    @Test
    public void malformedAndRandomAuthenticationDoesNotHarmTheMember() throws Exception {
        Config config = memberConfig();
        config.getSecurityConfig()
                .setClientRealmConfig("users", new RealmConfig()
                        .setSimpleAuthenticationConfig(new SimpleAuthenticationConfig().addUser("alice", "alice-pw")))
                .addClientPermissionConfig(new PermissionConfig(PermissionType.ALL, null, "*"));
        HecatonInstance member = Hecaton.newHecatonInstance(config);
        int port = member.getCluster().getLocalMember().getAddress().getPort();
        Random random = new Random(42);

        // random bytes after the client protocol header
        for (int i = 0; i < 200; i++) {
            byte[] garbage = new byte[1 + random.nextInt(4096)];
            random.nextBytes(garbage);
            try (Socket socket = new Socket("127.0.0.1", port)) {
                socket.setSoTimeout(2000);
                OutputStream out = socket.getOutputStream();
                out.write("CP2".getBytes(StandardCharsets.US_ASCII));
                out.write(garbage);
                out.flush();
            } catch (IOException expected) {
                // the member may close the connection first
                ignore(expected);
            }
        }
        // random credentials (each refused client retries until its connect timeout)
        for (int i = 0; i < 5; i++) {
            String user = randomString(random);
            String password = randomString(random);
            assertThatThrownBy(() -> client(member, user, password)).isInstanceOf(IllegalStateException.class);
        }

        assertThat(member.getLifecycleService().isRunning()).isTrue();
        client(member, "alice", "alice-pw").getMap("m").put("k", "v");
    }

    private static String randomString(Random random) {
        StringBuilder value = new StringBuilder();
        int length = random.nextInt(64);
        for (int i = 0; i < length; i++) {
            value.append((char) (random.nextInt(0xD7FF - 1) + 1));
        }
        return value.toString();
    }

    @Test
    public void joiningMemberReceivesTheClustersPermissions() {
        Config config1 = memberConfig();
        config1.getSecurityConfig().addClientPermissionConfig(permission(PermissionType.MAP, "a", "*", "read"));
        HecatonInstance member1 = Hecaton.newHecatonInstance(config1);
        Config config2 = memberConfig();
        config2.getSecurityConfig().addClientPermissionConfig(permission(PermissionType.MAP, "b", "*", "read"));
        HecatonInstance member2 = Hecaton.newHecatonInstance(config2);
        assertClusterSizeEventually(2, member1, member2);

        assertTrueEventually(() -> assertThat(permissionNames(member2)).containsExactly("a"));
        assertThat(permissionNames(member1)).containsExactly("a");
    }

    @Test
    public void joiningMemberCanSendItsPermissions() {
        Config config1 = memberConfig();
        config1.getSecurityConfig().addClientPermissionConfig(permission(PermissionType.MAP, "a", "*", "read"));
        HecatonInstance member1 = Hecaton.newHecatonInstance(config1);
        Config config2 = memberConfig();
        config2.getSecurityConfig().setOnJoinPermissionOperation(OnJoinPermissionOperationName.SEND)
                .addClientPermissionConfig(permission(PermissionType.MAP, "b", "*", "read"));
        HecatonInstance member2 = Hecaton.newHecatonInstance(config2);
        assertClusterSizeEventually(2, member1, member2);

        assertTrueEventually(() -> assertThat(permissionNames(member1)).containsExactly("b"));
        assertThat(permissionNames(member2)).containsExactly("b");
    }

    private static List<String> permissionNames(HecatonInstance member) {
        return ((SecurityContextImpl) getNode(member).securityContext).getClientPermissions().stream()
                .map(PermissionConfig::getName).toList();
    }

    @Test
    public void clientsAuthenticateWithTheirTlsCertificate() throws Exception {
        TestCertificates ca = new TestCertificates(folder.getRoot().toPath(), "ca");
        Path trustStore = ca.trustStore();
        Config config = memberConfig();
        config.getNetworkConfig().setSSLConfig(ssl(ca.keyStore("member", "127.0.0.1"), trustStore)
                .setProperty("mutualAuthentication", "REQUIRED"));
        config.getSecurityConfig()
                .setClientRealmConfig("certificates", new RealmConfig()
                        .setTlsAuthenticationConfig(new TlsAuthenticationConfig().setRoleAttribute("ou")))
                .addClientPermissionConfig(permission(PermissionType.MAP, "*", "operators", "all"));
        HecatonInstance member = Hecaton.newHecatonInstance(config);

        ClientConfig clientConfig = clientConfig(member);
        clientConfig.getNetworkConfig().setSSLConfig(ssl(ca.keyStore("operator,OU=operators"), trustStore));
        HecatonInstance operator = HecatonClient.newHecatonClient(clientConfig);
        operator.getMap("m").put("k", "v");

        ClientConfig otherConfig = clientConfig(member);
        otherConfig.getNetworkConfig().setSSLConfig(ssl(ca.keyStore("guest,OU=guests"), trustStore));
        HecatonInstance guest = HecatonClient.newHecatonClient(otherConfig);
        assertThatThrownBy(() -> guest.getMap("m").get("k")).isInstanceOf(AccessControlException.class);
    }

    private static PermissionConfig permission(PermissionType type, String name, String principal, String... actions) {
        PermissionConfig config = new PermissionConfig(type, name, principal);
        for (String action : actions) {
            config.addAction(action);
        }
        return config;
    }

    private static Config memberConfig() {
        Config config = smallInstanceConfigWithoutJetAndMetrics().setClusterName("secure");
        config.getNetworkConfig().setPort(5901);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        config.setSecurityConfig(new SecurityConfig().setEnabled(true));
        return config;
    }

    private static Config memberIdentityConfig(String password) {
        Config config = memberConfig();
        config.getSecurityConfig().setMemberRealmConfig("members",
                new RealmConfig().setUsernamePasswordIdentityConfig("member", password));
        return config;
    }

    private static HecatonInstance client(HecatonInstance member, String user, String password) {
        ClientConfig config = clientConfig(member);
        if (user != null) {
            config.getSecurityConfig().setUsernamePasswordIdentityConfig(user, password);
        }
        return HecatonClient.newHecatonClient(config);
    }

    private static ClientConfig clientConfig(HecatonInstance member) {
        ClientConfig config = new ClientConfig().setClusterName("secure");
        config.getNetworkConfig().addAddress("127.0.0.1:" + member.getCluster().getLocalMember().getAddress().getPort());
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(5000);
        return config;
    }

    private static SSLConfig ssl(Path keyStore, Path trustStore) {
        return new SSLConfig().setEnabled(true)
                .setProperty("keyStore", keyStore.toString())
                .setProperty("keyStorePassword", TestCertificates.PASSWORD)
                .setProperty("keyStoreType", "PKCS12")
                .setProperty("trustStore", trustStore.toString())
                .setProperty("trustStorePassword", TestCertificates.PASSWORD)
                .setProperty("trustStoreType", "PKCS12");
    }
}
