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

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import uk.org.hecaton.security.ClusterLoginModule;
import uk.org.hecaton.security.Credentials;
import uk.org.hecaton.security.TokenCredentials;
import uk.org.hecaton.security.impl.KerberosLogin;

import javax.security.auth.Subject;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

/**
 * Kerberos authentication ({@code kerberos} realm, F2): accepts the GSS-API token a client or member
 * presents (see {@code KerberosCredentialsFactory}) with the keys of the service {@code principal} from
 * {@code keytab-file}. The client's Kerberos principal (without {@code @REALM} with
 * {@code use-name-without-realm}) is its identity and a role; an LDAP realm configured in the Kerberos
 * realm maps further roles.
 */
public class GssApiLoginModule extends ClusterLoginModule {

    private String name;

    @Override
    protected boolean onLogin() throws LoginException {
        if (getStringOption("securityRealm", null) != null) {
            throw new LoginException("Acceptor credentials from another realm (security-realm) are not supported;"
                    + " configure principal and keytab-file");
        }
        Credentials credentials = getCredentials();
        if (!(credentials instanceof TokenCredentials tokenCredentials) || tokenCredentials.getToken() == null) {
            throw new FailedLoginException("A Kerberos token is required");
        }
        byte[] token = tokenCredentials.getToken();
        Subject service = KerberosLogin.login(getStringOption("principal", null), getStringOption("keytabFile", null),
                false);
        String principal = accept(service, token, getBoolOption("relaxFlagsCheck"));
        name = getBoolOption("useNameWithoutRealm") && principal.contains("@")
                ? principal.substring(0, principal.lastIndexOf('@')) : principal;
        addRole(name);
        // for an LDAP login module that maps the roles of the authenticated user
        @SuppressWarnings("unchecked")
        Map<String, Object> shared = (Map<String, Object>) sharedState;
        shared.put(BasicLdapLoginModule.SHARED_STATE_NAME, name);
        return true;
    }

    /** Accepts the token with the service's keys; returns the client's principal. */
    private static String accept(Subject service, byte[] token, boolean relaxFlagsCheck) throws LoginException {
        try {
            return Subject.doAs(service, (PrivilegedExceptionAction<String>) () -> {
                GSSContext context = GSSManager.getInstance().createContext((GSSCredential) null);
                try {
                    context.acceptSecContext(token, 0, token.length);
                    if (!context.isEstablished()) {
                        throw new FailedLoginException("The Kerberos token needs more than one round trip");
                    }
                    if (!relaxFlagsCheck && !context.getIntegState()) {
                        throw new FailedLoginException("The Kerberos context does not protect integrity");
                    }
                    return context.getSrcName().toString();
                } finally {
                    context.dispose();
                }
            });
        } catch (PrivilegedActionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof LoginException login) {
                throw login;
            }
            String reason = cause instanceof GSSException gss ? gss.getMajorString() : String.valueOf(cause);
            FailedLoginException failure = new FailedLoginException("Kerberos authentication failed: " + reason);
            failure.initCause(cause);
            throw failure;
        }
    }

    @Override
    protected String getName() {
        return name;
    }
}
