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

package uk.org.hecaton.security.loginimpl;

import uk.org.hecaton.config.security.LdapRoleMappingMode;
import uk.org.hecaton.config.security.LdapSearchScope;
import uk.org.hecaton.security.ClusterLoginModule;
import uk.org.hecaton.security.Credentials;
import uk.org.hecaton.security.PasswordCredentials;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * LDAP authentication without a system account ({@code ldap} realm without {@code system-user-dn}):
 * the client's user name is the DN of its LDAP entry, and its password is checked by binding as that
 * entry. Roles come from the entry, by {@code role-mapping-mode}:
 * <ul>
 *     <li>{@code attribute}: the values of {@code role-mapping-attribute} are the role names;</li>
 *     <li>{@code direct}: the values of {@code role-mapping-attribute} are DNs of role entries, whose
 *     {@code role-name-attribute} gives the role name (followed recursively up to
 *     {@code role-recursion-max-depth});</li>
 *     <li>{@code reverse}: the role entries found under {@code role-context} by {@code role-filter}
 *     ({@code {memberDN}} replaced with the user's DN), recursively.</li>
 * </ul>
 * With {@code parse-dn}, a role DN gives the role name directly: the value of its first RDN whose type
 * is {@code role-name-attribute}. The identity's name is the value of {@code user-name-attribute}, or the DN.
 */
public class BasicLdapLoginModule extends ClusterLoginModule {

    static final String OPTION_PARSE_DN = "parseDN";
    static final String OPTION_ROLE_CONTEXT = "roleContext";
    static final String OPTION_ROLE_FILTER = "roleFilter";
    static final String OPTION_ROLE_MAPPING_ATTRIBUTE = "roleMappingAttribute";
    static final String OPTION_ROLE_MAPPING_MODE = "roleMappingMode";
    static final String OPTION_ROLE_NAME_ATTRIBUTE = "roleNameAttribute";
    static final String OPTION_ROLE_RECURSION_MAX_DEPTH = "roleRecursionMaxDepth";
    static final String OPTION_ROLE_SEARCH_SCOPE = "roleSearchScope";
    static final String OPTION_USER_NAME_ATTRIBUTE = "userNameAttribute";
    static final String DEFAULT_ROLE_FILTER = "(member={memberDN})";
    /** The shared state key under which a login module that authenticated the client puts its name. */
    static final String SHARED_STATE_NAME = "hecaton.authenticatedName";

    private String name;

    @Override
    protected boolean onLogin() throws LoginException {
        PasswordCredentials credentials = passwordCredentials();
        DirContext context = connect(credentials.getName(), credentials.getPassword());
        try {
            identify(context, credentials.getName());
        } finally {
            close(context);
        }
        return true;
    }

    @Override
    protected String getName() {
        return name;
    }

    PasswordCredentials passwordCredentials() throws LoginException {
        Credentials credentials = getCredentials();
        if (!(credentials instanceof PasswordCredentials password) || isEmpty(password.getName())
                || isEmpty(password.getPassword())) {
            // an empty password would be an anonymous bind, which LDAP servers accept
            throw new FailedLoginException("A user name and password are required");
        }
        return password;
    }

    /** Binds to the LDAP server with the given DN and password (or anonymously when both are null). */
    DirContext connect(String dn, String password) throws LoginException {
        Hashtable<String, Object> environment = new Hashtable<>();
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        for (Map.Entry<String, ?> option : options.entrySet()) {
            if (option.getKey().startsWith("java.naming.") || option.getKey().startsWith("com.sun.jndi.")) {
                environment.put(option.getKey(), option.getValue());
            }
        }
        if (dn != null) {
            environment.put(Context.SECURITY_AUTHENTICATION, "simple");
            environment.put(Context.SECURITY_PRINCIPAL, dn);
            environment.put(Context.SECURITY_CREDENTIALS, password);
        }
        try {
            return new InitialDirContext(environment);
        } catch (javax.naming.AuthenticationException e) {
            throw new FailedLoginException("Invalid user name or password");
        } catch (NamingException e) {
            LoginException failure = new LoginException("The LDAP server could not be reached: " + e.getMessage());
            failure.initCause(e);
            throw failure;
        }
    }

    /** Sets the name and adds the roles of the user entry {@code userDn}, read with {@code context}. */
    void identify(DirContext context, String userDn) throws LoginException {
        try {
            Attributes attributes = context.getAttributes(userDn);
            String userNameAttribute = getStringOption(OPTION_USER_NAME_ATTRIBUTE, null);
            Attribute userName = userNameAttribute == null ? null : attributes.get(userNameAttribute);
            name = userName == null ? userDn : String.valueOf(userName.get());
            for (String role : roles(context, userDn, attributes)) {
                addRole(role);
            }
        } catch (NamingException e) {
            LoginException failure = new LoginException("Cannot read the LDAP entry " + userDn + ": " + e.getMessage());
            failure.initCause(e);
            throw failure;
        }
    }

    private Set<String> roles(DirContext context, String userDn, Attributes userAttributes) throws NamingException {
        LdapRoleMappingMode mode = LdapRoleMappingMode.getRoleMappingMode(getStringOption(OPTION_ROLE_MAPPING_MODE, null));
        String mappingAttribute = getStringOption(OPTION_ROLE_MAPPING_ATTRIBUTE, null);
        Set<String> roles = new LinkedHashSet<>();
        switch (mode) {
            case ATTRIBUTE -> roles.addAll(values(userAttributes, mappingAttribute));
            case DIRECT -> {
                if (mappingAttribute != null) {
                    followRoles(context, values(userAttributes, mappingAttribute), roles);
                }
            }
            case REVERSE -> searchRoles(context, userDn, roles);
            default -> throw new IllegalArgumentException("Unknown role mapping mode " + mode);
        }
        return roles;
    }

    /** Direct mapping: role entries named by the mapping attribute, recursively. */
    private void followRoles(DirContext context, Set<String> roleDns, Set<String> roles) throws NamingException {
        String mappingAttribute = getStringOption(OPTION_ROLE_MAPPING_ATTRIBUTE, null);
        int maxDepth = maxDepth();
        Set<String> visited = new HashSet<>();
        Deque<String[]> pending = new ArrayDeque<>();
        roleDns.forEach(dn -> pending.add(new String[] {dn, "1"}));
        while (!pending.isEmpty()) {
            String[] next = pending.poll();
            String roleDn = next[0];
            int depth = Integer.parseInt(next[1]);
            if (!visited.add(roleDn)) {
                continue;
            }
            Attributes attributes = parseDn() ? null : context.getAttributes(roleDn);
            addRoleName(roleDn, attributes, roles);
            if (depth < maxDepth) {
                Attributes roleAttributes = attributes != null ? attributes : context.getAttributes(roleDn);
                for (String member : values(roleAttributes, mappingAttribute)) {
                    pending.add(new String[] {member, String.valueOf(depth + 1)});
                }
            }
        }
    }

    /** Reverse mapping: role entries whose filter matches the user, then the roles matching those, recursively. */
    private void searchRoles(DirContext context, String userDn, Set<String> roles) throws NamingException {
        String roleContext = getStringOption(OPTION_ROLE_CONTEXT, null);
        if (roleContext == null) {
            return;
        }
        String filter = getStringOption(OPTION_ROLE_FILTER, DEFAULT_ROLE_FILTER);
        SearchControls controls = new SearchControls();
        controls.setSearchScope(LdapSearchScope.getSearchScope(getStringOption(OPTION_ROLE_SEARCH_SCOPE, null))
                .toSearchControlValue());
        Set<String> visited = new HashSet<>();
        Deque<String> members = new ArrayDeque<>();
        members.add(userDn);
        for (int depth = 1; depth <= maxDepth() && !members.isEmpty(); depth++) {
            Deque<String> next = new ArrayDeque<>();
            for (String member : members) {
                NamingEnumeration<SearchResult> results = context.search(roleContext,
                        filter.replace("{memberDN}", "{0}"), new Object[] {member}, controls);
                while (results.hasMore()) {
                    SearchResult result = results.next();
                    String roleDn = result.getNameInNamespace();
                    if (visited.add(roleDn)) {
                        addRoleName(roleDn, result.getAttributes(), roles);
                        next.add(roleDn);
                    }
                }
            }
            members = next;
        }
    }

    private void addRoleName(String roleDn, Attributes attributes, Set<String> roles) throws NamingException {
        String nameAttribute = getStringOption(OPTION_ROLE_NAME_ATTRIBUTE, null);
        if (nameAttribute == null) {
            roles.add(roleDn);
        } else if (attributes == null) {
            // the RDNs from the most significant; the first one of the DN is the last
            List<Rdn> rdns = new LdapName(roleDn).getRdns();
            for (int i = rdns.size() - 1; i >= 0; i--) {
                Rdn rdn = rdns.get(i);
                if (rdn.getType().equalsIgnoreCase(nameAttribute)) {
                    roles.add(String.valueOf(rdn.getValue()));
                    return;
                }
            }
        } else {
            roles.addAll(values(attributes, nameAttribute));
        }
    }

    private boolean parseDn() {
        return Boolean.parseBoolean(getStringOption(OPTION_PARSE_DN, "false"));
    }

    private int maxDepth() {
        return Integer.parseInt(getStringOption(OPTION_ROLE_RECURSION_MAX_DEPTH, "1"));
    }

    static Set<String> values(Attributes attributes, String attributeName) throws NamingException {
        Set<String> values = new LinkedHashSet<>();
        Attribute attribute = attributeName == null || attributes == null ? null : attributes.get(attributeName);
        if (attribute != null) {
            NamingEnumeration<?> all = attribute.getAll();
            while (all.hasMore()) {
                Object value = all.next();
                values.add(value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(value));
            }
        }
        return values;
    }

    static void close(DirContext context) {
        try {
            context.close();
        } catch (NamingException ignored) {
            // nothing to do
        }
    }

    static boolean isEmpty(String value) {
        return value == null || value.isEmpty();
    }
}
