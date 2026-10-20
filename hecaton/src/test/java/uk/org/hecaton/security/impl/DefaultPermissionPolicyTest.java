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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.PermissionConfig;
import uk.org.hecaton.config.PermissionConfig.PermissionType;
import uk.org.hecaton.security.ClusterEndpointPrincipal;
import uk.org.hecaton.security.ClusterIdentityPrincipal;
import uk.org.hecaton.security.ClusterRolePrincipal;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.JobPermission;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.security.permission.QueuePermission;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.security.auth.Subject;
import java.security.Permission;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultPermissionPolicyTest {

    private static final Permission READ_ORDERS = new MapPermission("orders", ActionConstants.ACTION_READ);
    private static final Permission PUT_ORDERS = new MapPermission("orders", ActionConstants.ACTION_PUT);

    @Test
    public void matchesNamesWithWildcards() {
        DefaultPermissionPolicy policy = policy(false, permission(PermissionType.MAP, "ord*", "*", "read"));
        assertThat(allows(policy, subject("alice", "10.0.0.1"), READ_ORDERS)).isTrue();
        assertThat(allows(policy, subject("alice", "10.0.0.1"), PUT_ORDERS)).isFalse();
        assertThat(allows(policy, subject("alice", "10.0.0.1"), new MapPermission("customers", "read"))).isFalse();
        assertThat(allows(policy, subject("alice", "10.0.0.1"), new QueuePermission("orders", "read"))).isFalse();
    }

    @Test
    public void matchesPrincipalsByIdentityOrRole() {
        DefaultPermissionPolicy policy = policy(false,
                permission(PermissionType.MAP, "orders", "alice", "read"),
                permission(PermissionType.MAP, "orders", "writers", "put"));
        assertThat(allows(policy, subject("alice", "10.0.0.1"), READ_ORDERS)).isTrue();
        assertThat(allows(policy, subject("bob", "10.0.0.1"), READ_ORDERS)).isFalse();
        assertThat(allows(policy, subject("bob", "10.0.0.1", "writers"), PUT_ORDERS)).isTrue();
    }

    @Test
    public void matchesEndpoints() {
        PermissionConfig config = permission(PermissionType.MAP, "orders", "*", "read").addEndpoint("10.0.0.*");
        DefaultPermissionPolicy policy = policy(false, config);
        assertThat(allows(policy, subject("alice", "10.0.0.7"), READ_ORDERS)).isTrue();
        assertThat(allows(policy, subject("alice", "192.168.1.7"), READ_ORDERS)).isFalse();
    }

    @Test
    public void denyWinsUnlessGrantsHavePriority() {
        PermissionConfig all = new PermissionConfig(PermissionType.ALL, null, "*");
        PermissionConfig deny = permission(PermissionType.MAP, "orders", "*", "read").setDeny(true);
        assertThat(allows(policy(false, all, deny), subject("alice", "10.0.0.1"), READ_ORDERS)).isFalse();
        assertThat(allows(policy(false, all, deny), subject("alice", "10.0.0.1"), PUT_ORDERS)).isTrue();
        assertThat(allows(policy(true, all, deny), subject("alice", "10.0.0.1"), READ_ORDERS)).isTrue();
    }

    @Test
    public void createsPermissionsWithoutNames() {
        DefaultPermissionPolicy policy = policy(false, permission(PermissionType.JOB, null, "*", "submit"));
        assertThat(allows(policy, subject("alice", "10.0.0.1"), new JobPermission(ActionConstants.ACTION_SUBMIT))).isTrue();
    }

    private static boolean allows(DefaultPermissionPolicy policy, Subject subject, Permission permission) {
        return policy.getPermissions(subject, permission.getClass()).implies(permission);
    }

    private static DefaultPermissionPolicy policy(boolean grantPriority, PermissionConfig... permissions) {
        Config config = new Config();
        config.getSecurityConfig().setPermissionPriorityGrant(grantPriority);
        for (PermissionConfig permission : permissions) {
            config.getSecurityConfig().addClientPermissionConfig(permission);
        }
        DefaultPermissionPolicy policy = new DefaultPermissionPolicy();
        policy.configure(config, null);
        return policy;
    }

    private static PermissionConfig permission(PermissionType type, String name, String principal, String... actions) {
        PermissionConfig config = new PermissionConfig(type, name, principal);
        for (String action : actions) {
            config.addAction(action);
        }
        return config;
    }

    private static Subject subject(String identity, String endpoint, String... roles) {
        Subject subject = new Subject();
        subject.getPrincipals().add(new ClusterIdentityPrincipal(identity));
        subject.getPrincipals().add(new ClusterEndpointPrincipal(endpoint));
        for (String role : roles) {
            subject.getPrincipals().add(new ClusterRolePrincipal(role));
        }
        return subject;
    }
}
