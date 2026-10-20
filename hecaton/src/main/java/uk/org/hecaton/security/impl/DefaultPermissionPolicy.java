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
import uk.org.hecaton.config.matcher.WildcardConfigPatternMatcher;
import uk.org.hecaton.internal.util.AddressUtil;
import uk.org.hecaton.security.ClusterEndpointPrincipal;
import uk.org.hecaton.security.ClusterIdentityPrincipal;
import uk.org.hecaton.security.ClusterRolePrincipal;
import uk.org.hecaton.security.IPermissionPolicy;
import uk.org.hecaton.security.permission.AllPermissions;
import uk.org.hecaton.security.permission.AtomicLongPermission;
import uk.org.hecaton.security.permission.AtomicReferencePermission;
import uk.org.hecaton.security.permission.CPMapPermission;
import uk.org.hecaton.security.permission.CachePermission;
import uk.org.hecaton.security.permission.CardinalityEstimatorPermission;
import uk.org.hecaton.security.permission.ConfigPermission;
import uk.org.hecaton.security.permission.ConnectorPermission;
import uk.org.hecaton.security.permission.CountDownLatchPermission;
import uk.org.hecaton.security.permission.DurableExecutorServicePermission;
import uk.org.hecaton.security.permission.ExecutorServicePermission;
import uk.org.hecaton.security.permission.FlakeIdGeneratorPermission;
import uk.org.hecaton.security.permission.JobPermission;
import uk.org.hecaton.security.permission.ListPermission;
import uk.org.hecaton.security.permission.LockPermission;
import uk.org.hecaton.security.permission.ManagementPermission;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.security.permission.MultiMapPermission;
import uk.org.hecaton.security.permission.PNCounterPermission;
import uk.org.hecaton.security.permission.QueuePermission;
import uk.org.hecaton.security.permission.ReliableTopicPermission;
import uk.org.hecaton.security.permission.ReplicatedMapPermission;
import uk.org.hecaton.security.permission.RingBufferPermission;
import uk.org.hecaton.security.permission.ScheduledExecutorPermission;
import uk.org.hecaton.security.permission.SemaphorePermission;
import uk.org.hecaton.security.permission.SetPermission;
import uk.org.hecaton.security.permission.SqlPermission;
import uk.org.hecaton.security.permission.TopicPermission;
import uk.org.hecaton.security.permission.TransactionPermission;
import uk.org.hecaton.security.permission.UserCodeDeploymentPermission;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;
import uk.org.hecaton.security.permission.VectorCollectionPermission;

import javax.security.auth.Subject;
import java.lang.reflect.Constructor;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * The client permission policy used unless another is configured (F3): it grants a client the
 * {@link PermissionConfig}s whose principal matches one of its identity or role principals
 * ({@code *} or no principal matches all) and whose endpoints match its address (no endpoints
 * match all). Deny permissions take precedence over grants, unless the security configuration
 * gives grants priority.
 */
public class DefaultPermissionPolicy implements IPermissionPolicy {

    private static final WildcardConfigPatternMatcher MATCHER = new WildcardConfigPatternMatcher();
    private static final Map<PermissionType, Class<? extends Permission>> PERMISSION_CLASSES = permissionClasses();
    private static final List<List<Class<?>>> CONSTRUCTOR_SIGNATURES = List.of(
            List.of(String.class, String[].class), List.of(String[].class), List.of(String.class), List.of());

    private volatile List<Rule> rules = Collections.emptyList();
    private boolean grantPriority;

    /** A permission config, ready to evaluate. */
    private record Rule(String principal, Set<String> endpoints, Permission permission, boolean deny) {
    }

    @Override
    public void configure(Config config, Properties properties) {
        grantPriority = config.getSecurityConfig().isPermissionPriorityGrant();
        refreshPermissions(config.getSecurityConfig().getClientPermissionConfigs());
    }

    @Override
    public PermissionCollection getPermissions(Subject subject, Class<? extends Permission> type) {
        List<Permission> grants = new ArrayList<>();
        List<Permission> denies = new ArrayList<>();
        for (Rule rule : rules) {
            if ((rule.permission instanceof AllPermissions || type.isInstance(rule.permission)) && applies(rule, subject)) {
                (rule.deny ? denies : grants).add(rule.permission);
            }
        }
        return new SubjectPermissions(grants, denies, grantPriority);
    }

    @Override
    public void refreshPermissions(Set<PermissionConfig> permissionConfigs) {
        List<Rule> newRules = new ArrayList<>();
        for (PermissionConfig config : permissionConfigs) {
            newRules.add(new Rule(config.getPrincipal(), config.getEndpoints(), toPermission(config), config.isDeny()));
        }
        rules = List.copyOf(newRules);
    }

    @Override
    public void destroy() {
        rules = Collections.emptyList();
    }

    private static boolean applies(Rule rule, Subject subject) {
        return principalMatches(rule.principal, subject) && endpointMatches(rule.endpoints, subject);
    }

    private static boolean principalMatches(String pattern, Subject subject) {
        if (pattern == null || pattern.isBlank() || "*".equals(pattern.trim())) {
            return true;
        }
        for (Principal principal : subject.getPrincipals()) {
            if ((principal instanceof ClusterIdentityPrincipal || principal instanceof ClusterRolePrincipal)
                    && MATCHER.matches(pattern.trim(), principal.getName())) {
                return true;
            }
        }
        return false;
    }

    private static boolean endpointMatches(Set<String> endpoints, Subject subject) {
        if (endpoints == null || endpoints.isEmpty()) {
            return true;
        }
        for (ClusterEndpointPrincipal principal : subject.getPrincipals(ClusterEndpointPrincipal.class)) {
            if (AddressUtil.matchAnyInterface(principal.getName(), endpoints)) {
                return true;
            }
        }
        return false;
    }

    /** The {@link Permission} a permission config describes. */
    static Permission toPermission(PermissionConfig config) {
        Class<? extends Permission> type = PERMISSION_CLASSES.get(config.getType());
        if (type == null) {
            throw new IllegalArgumentException("Unknown permission type " + config.getType());
        }
        String name = config.getName() == null || config.getName().isBlank() ? "*" : config.getName();
        String[] actions = config.getActions() == null ? new String[0] : config.getActions().toArray(new String[0]);
        try {
            // constructors in order of preference: (name, actions), (actions), (name), ()
            for (List<Class<?>> signature : CONSTRUCTOR_SIGNATURES) {
                Constructor<?> constructor = constructor(type, signature);
                if (constructor != null) {
                    return (Permission) constructor.newInstance(arguments(signature, name, actions));
                }
            }
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Cannot create " + type.getName() + " for " + config, e);
        }
        throw new IllegalArgumentException("No usable constructor in " + type.getName());
    }

    private static Constructor<?> constructor(Class<?> type, List<Class<?>> signature) {
        try {
            return type.getConstructor(signature.toArray(new Class<?>[0]));
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private static Object[] arguments(List<Class<?>> signature, String name, String[] actions) {
        Object[] arguments = new Object[signature.size()];
        for (int i = 0; i < arguments.length; i++) {
            arguments[i] = signature.get(i) == String.class ? name : actions;
        }
        return arguments;
    }

    private static Map<PermissionType, Class<? extends Permission>> permissionClasses() {
        List<Class<? extends Permission>> classes = List.of(AllPermissions.class, MapPermission.class,
                QueuePermission.class, TopicPermission.class, MultiMapPermission.class, ListPermission.class,
                SetPermission.class, FlakeIdGeneratorPermission.class, LockPermission.class, AtomicLongPermission.class,
                AtomicReferencePermission.class, CountDownLatchPermission.class, SemaphorePermission.class,
                ExecutorServicePermission.class, TransactionPermission.class, DurableExecutorServicePermission.class,
                CardinalityEstimatorPermission.class, ScheduledExecutorPermission.class, CachePermission.class,
                UserCodeDeploymentPermission.class, ConfigPermission.class, PNCounterPermission.class,
                RingBufferPermission.class, ReliableTopicPermission.class, ReplicatedMapPermission.class,
                ManagementPermission.class, JobPermission.class, ConnectorPermission.class, SqlPermission.class,
                CPMapPermission.class, UserCodeNamespacePermission.class, VectorCollectionPermission.class);
        Map<PermissionType, Class<? extends Permission>> map = new EnumMap<>(PermissionType.class);
        for (Class<? extends Permission> type : classes) {
            map.put(PermissionType.getTypeByPermissionClassName(type.getName()), type);
        }
        return map;
    }

    /** The permissions of one subject for one permission type. */
    static final class SubjectPermissions extends PermissionCollection {

        private static final long serialVersionUID = 1L;

        private final List<Permission> grants;
        private final List<Permission> denies;
        private final boolean grantPriority;

        SubjectPermissions(List<Permission> grants, List<Permission> denies, boolean grantPriority) {
            this.grants = grants;
            this.denies = denies;
            this.grantPriority = grantPriority;
            setReadOnly();
        }

        @Override
        public void add(Permission permission) {
            throw new SecurityException("read-only");
        }

        @Override
        public boolean implies(Permission permission) {
            boolean granted = grants.stream().anyMatch(p -> p.implies(permission));
            if (!granted) {
                return false;
            }
            return grantPriority || denies.stream().noneMatch(p -> p.implies(permission));
        }

        @Override
        public Enumeration<Permission> elements() {
            return Collections.enumeration(grants);
        }
    }
}
