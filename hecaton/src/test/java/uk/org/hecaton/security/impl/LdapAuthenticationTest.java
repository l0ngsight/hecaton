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

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.PermissionConfig;
import uk.org.hecaton.config.PermissionConfig.PermissionType;
import uk.org.hecaton.config.SecurityConfig;
import uk.org.hecaton.config.security.LdapAuthenticationConfig;
import uk.org.hecaton.config.security.LdapRoleMappingMode;
import uk.org.hecaton.config.security.RealmConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.security.AccessControlException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * LDAP authentication (F2): clients are authenticated against an LDAP server and get roles from it.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class LdapAuthenticationTest extends HecatonTestSupport {

    private static final String BASE = "dc=example,dc=com";
    private static final String ALICE = "uid=alice,ou=users," + BASE;
    private static final String BOB = "uid=bob,ou=users," + BASE;

    private InMemoryDirectoryServer ldap;
    private String url;

    @Before
    public void startLdap() throws Exception {
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig(BASE);
        config.addAdditionalBindCredentials("cn=admin", "admin-pw");
        config.setListenerConfigs(InMemoryListenerConfig.createLDAPConfig("ldap", 0));
        config.setSchema(null);
        ldap = new InMemoryDirectoryServer(config);
        ldap.add("dn: " + BASE, "objectClass: top", "objectClass: domain", "dc: example");
        ldap.add("dn: ou=users," + BASE, "objectClass: organizationalUnit", "ou: users");
        ldap.add("dn: ou=groups," + BASE, "objectClass: organizationalUnit", "ou: groups");
        ldap.add("dn: " + ALICE, "objectClass: inetOrgPerson", "uid: alice", "cn: Alice", "sn: A",
                "userPassword: alice-pw", "employeeType: admins", "memberOf: cn=admins,ou=groups," + BASE);
        ldap.add("dn: " + BOB, "objectClass: inetOrgPerson", "uid: bob", "cn: Bob", "sn: B",
                "userPassword: bob-pw", "employeeType: readers");
        ldap.add("dn: cn=admins,ou=groups," + BASE, "objectClass: groupOfNames", "cn: admins", "member: " + ALICE,
                "memberOf: cn=operators,ou=groups," + BASE);
        ldap.add("dn: cn=operators,ou=groups," + BASE, "objectClass: groupOfNames", "cn: operators",
                "member: cn=admins,ou=groups," + BASE);
        ldap.startListening();
        url = "ldap://127.0.0.1:" + ldap.getListenPort();
    }

    @After
    public void tearDown() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
        ldap.shutDown(true);
    }

    @Test
    public void basicLdapRolesFromAnAttribute() {
        // the user name is the DN; roles are the values of employeeType
        HecatonInstance member = start(new LdapAuthenticationConfig().setUrl(url)
                .setRoleMappingMode(LdapRoleMappingMode.ATTRIBUTE).setRoleMappingAttribute("employeeType"));

        client(member, ALICE, "alice-pw").getMap("data").put("k", "v");
        HecatonInstance bob = client(member, BOB, "bob-pw");
        assertThat(bob.getMap("data").get("k")).isEqualTo("v");
        assertThatThrownBy(() -> bob.getMap("data").put("k", "x")).isInstanceOf(AccessControlException.class);
        assertThatThrownBy(() -> client(member, ALICE, "wrong")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> client(member, ALICE, "")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void systemAccountWithDirectRoleMapping() {
        // the user is found by uid; memberOf names role entries, followed one more level
        HecatonInstance member = start(new LdapAuthenticationConfig().setUrl(url)
                .setSystemUserDn("cn=admin").setSystemUserPassword("admin-pw")
                .setUserContext("ou=users," + BASE).setUserFilter("(uid={login})")
                .setRoleMappingMode(LdapRoleMappingMode.DIRECT).setRoleMappingAttribute("memberOf")
                .setRoleNameAttribute("cn").setRoleRecursionMaxDepth(2), "operators");

        client(member, "alice", "alice-pw").getMap("data").put("k", "v");
        assertThatThrownBy(() -> client(member, "bob", "bob-pw").getMap("data").put("k", "x"))
                .isInstanceOf(AccessControlException.class);
        assertThatThrownBy(() -> client(member, "alice", "wrong")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> client(member, "mallory", "x")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void reverseRoleMappingByGroupMembership() {
        HecatonInstance member = start(new LdapAuthenticationConfig().setUrl(url)
                .setSystemUserDn("cn=admin").setSystemUserPassword("admin-pw")
                .setUserContext("ou=users," + BASE)
                .setRoleMappingMode(LdapRoleMappingMode.REVERSE).setRoleContext("ou=groups," + BASE)
                .setRoleFilter("(member={memberDN})").setRoleNameAttribute("cn").setRoleRecursionMaxDepth(2), "operators");

        client(member, "alice", "alice-pw").getMap("data").put("k", "v");
        assertThatThrownBy(() -> client(member, "bob", "bob-pw").getMap("data").put("k", "x"))
                .isInstanceOf(AccessControlException.class);
    }

    @Test
    public void passwordAttributeComparison() {
        HecatonInstance member = start(new LdapAuthenticationConfig().setUrl(url)
                .setSystemUserDn("cn=admin").setSystemUserPassword("admin-pw")
                .setUserContext("ou=users," + BASE).setPasswordAttribute("userPassword")
                .setRoleMappingAttribute("employeeType"));

        client(member, "alice", "alice-pw").getMap("data").put("k", "v");
        assertThatThrownBy(() -> client(member, "alice", "wrong")).isInstanceOf(IllegalStateException.class);
    }

    private HecatonInstance start(LdapAuthenticationConfig ldapConfig) {
        return start(ldapConfig, "admins");
    }

    private HecatonInstance start(LdapAuthenticationConfig ldapConfig, String adminRole) {
        Config config = smallInstanceConfigWithoutJetAndMetrics().setClusterName("ldap");
        config.getNetworkConfig().setPort(5911);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.setSecurityConfig(new SecurityConfig().setEnabled(true)
                .setClientRealmConfig("ldap", new RealmConfig().setLdapAuthenticationConfig(ldapConfig))
                .addClientPermissionConfig(new PermissionConfig(PermissionType.ALL, null, adminRole))
                .addClientPermissionConfig(new PermissionConfig(PermissionType.MAP, "data", "*").addAction("create")
                        .addAction("read")));
        return Hecaton.newHecatonInstance(config);
    }

    private static HecatonInstance client(HecatonInstance member, String user, String password) {
        ClientConfig config = new ClientConfig().setClusterName("ldap");
        config.getNetworkConfig().addAddress("127.0.0.1:" + member.getCluster().getLocalMember().getAddress().getPort());
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(3000);
        config.getSecurityConfig().setUsernamePasswordIdentityConfig(user, password);
        return HecatonClient.newHecatonClient(config);
    }
}
