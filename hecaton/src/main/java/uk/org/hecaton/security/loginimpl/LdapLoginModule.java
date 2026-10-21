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

import uk.org.hecaton.config.security.LdapSearchScope;
import uk.org.hecaton.security.PasswordCredentials;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * LDAP authentication with a system account ({@code ldap} realm with {@code system-user-dn}): the
 * member binds as the system user, searches {@code user-context} with {@code user-filter}
 * ({@code {login}} replaced with the client's user name) for the client's entry, then checks the
 * password by binding as that entry, or by comparing it with {@code password-attribute}, or not at
 * all with {@code skip-authentication} (when another realm authenticated the client). Roles are
 * mapped as in {@link BasicLdapLoginModule}, with the system account.
 */
public class LdapLoginModule extends BasicLdapLoginModule {

    static final String OPTION_USER_CONTEXT = "userContext";
    static final String OPTION_USER_FILTER = "userFilter";
    static final String OPTION_USER_SEARCH_SCOPE = "userSearchScope";
    static final String OPTION_PASSWORD_ATTRIBUTE = "passwordAttribute";
    static final String OPTION_SKIP_AUTHENTICATION = "skipAuthentication";
    static final String OPTION_SECURITY_REALM = "securityRealm";
    static final String DEFAULT_USER_FILTER = "(uid={login})";

    @Override
    protected boolean onLogin() throws LoginException {
        if (getStringOption(OPTION_SECURITY_REALM, null) != null) {
            throw new LoginException("System authentication through another realm (security-realm) is not supported;"
                    + " configure system-user-dn and system-user-password");
        }
        boolean skipAuthentication = getBoolOption(OPTION_SKIP_AUTHENTICATION);
        String login;
        String password = null;
        if (skipAuthentication) {
            // another login module (Kerberos) authenticated the client
            Object authenticated = sharedState.get(SHARED_STATE_NAME);
            login = authenticated != null ? authenticated.toString() : getCredentials().getName();
        } else {
            PasswordCredentials credentials = passwordCredentials();
            login = credentials.getName();
            password = credentials.getPassword();
        }
        // the system account: the java.naming.security.* options
        DirContext system = connect(null, null);
        try {
            String userDn = findUser(system, login);
            if (!skipAuthentication) {
                checkPassword(system, userDn, password);
            }
            identify(system, userDn);
        } finally {
            close(system);
        }
        return true;
    }

    private String findUser(DirContext system, String login) throws LoginException {
        SearchControls controls = new SearchControls();
        controls.setSearchScope(LdapSearchScope.getSearchScope(getStringOption(OPTION_USER_SEARCH_SCOPE, null))
                .toSearchControlValue());
        controls.setReturningAttributes(new String[0]);
        String filter = getStringOption(OPTION_USER_FILTER, DEFAULT_USER_FILTER).replace("{login}", "{0}");
        try {
            NamingEnumeration<SearchResult> results = system.search(getStringOption(OPTION_USER_CONTEXT, ""), filter,
                    new Object[] {login}, controls);
            if (!results.hasMore()) {
                throw new FailedLoginException("Invalid user name or password");
            }
            String userDn = results.next().getNameInNamespace();
            if (results.hasMore()) {
                throw new FailedLoginException("More than one LDAP entry matches the user name");
            }
            return userDn;
        } catch (NamingException e) {
            LoginException failure = new LoginException("Cannot search the LDAP server: " + e.getMessage());
            failure.initCause(e);
            throw failure;
        }
    }

    private void checkPassword(DirContext system, String userDn, String password) throws LoginException {
        String passwordAttribute = getStringOption(OPTION_PASSWORD_ATTRIBUTE, null);
        if (passwordAttribute == null) {
            close(connect(userDn, password));
            return;
        }
        try {
            for (String stored : values(system.getAttributes(userDn, new String[] {passwordAttribute}), passwordAttribute)) {
                if (MessageDigest.isEqual(stored.getBytes(StandardCharsets.UTF_8), password.getBytes(StandardCharsets.UTF_8))) {
                    return;
                }
            }
        } catch (NamingException e) {
            LoginException failure = new LoginException("Cannot read the password of " + userDn + ": " + e.getMessage());
            failure.initCause(e);
            throw failure;
        }
        throw new FailedLoginException("Invalid user name or password");
    }
}
