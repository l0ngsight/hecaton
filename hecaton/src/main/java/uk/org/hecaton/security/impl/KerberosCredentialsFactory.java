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

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.security.Credentials;
import uk.org.hecaton.security.ICredentialsFactory;
import uk.org.hecaton.security.SimpleTokenCredentials;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

/**
 * Kerberos identity ({@code kerberos} identity of a client or member realm, F2): presents a
 * GSS-API token for the service principal of the member it connects to, which a member with a
 * {@code kerberos} authentication realm accepts. The service principal is {@code spn}, or
 * {@code service-name-prefix} (default {@value #DEFAULT_SERVICE_NAME_PREFIX}, as in Hazelcast) followed by the
 * member's host name and {@code @realm}. The ticket comes from the {@code principal}'s
 * {@code keytab-file}, or from the ticket cache.
 */
public class KerberosCredentialsFactory implements ICredentialsFactory {

    /** The default prefix of the service principal names. */
    public static final String DEFAULT_SERVICE_NAME_PREFIX = "hz/";

    private static final String KRB5_MECHANISM = "1.2.840.113554.1.2.2";

    private Properties properties = new Properties();

    @Override
    public void init(Properties properties) {
        this.properties = properties == null ? new Properties() : properties;
    }

    @Override
    public void configure(CallbackHandler callbackHandler) {
        // nothing to configure
    }

    @Override
    public Credentials newCredentials() {
        return newCredentials(null);
    }

    @Override
    public Credentials newCredentials(Address address) {
        String servicePrincipal = servicePrincipal(address);
        try {
            Subject subject = KerberosLogin.login(properties.getProperty("principal"), properties.getProperty("keytabFile"),
                    true);
            byte[] token = Subject.doAs(subject, (PrivilegedExceptionAction<byte[]>) () -> {
                GSSManager manager = GSSManager.getInstance();
                Oid krb5 = new Oid(KRB5_MECHANISM);
                GSSName service = manager.createName(servicePrincipal, GSSName.NT_USER_NAME, krb5);
                GSSContext context = manager.createContext(service, krb5, null, GSSContext.DEFAULT_LIFETIME);
                try {
                    context.requestMutualAuth(false);
                    context.requestInteg(true);
                    return context.initSecContext(new byte[0], 0, 0);
                } finally {
                    context.dispose();
                }
            });
            return new SimpleTokenCredentials(token);
        } catch (LoginException | java.security.PrivilegedActionException e) {
            throw new SecurityException("Cannot get a Kerberos ticket for " + servicePrincipal + ": "
                    + rootMessage(e), e);
        }
    }

    private String servicePrincipal(Address address) {
        String spn = properties.getProperty("spn");
        if (spn != null) {
            return spn;
        }
        if (address == null) {
            throw new IllegalStateException("The Kerberos identity needs an spn when the target is not known");
        }
        String prefix = properties.getProperty("serviceNamePrefix", DEFAULT_SERVICE_NAME_PREFIX);
        boolean canonical = Boolean.parseBoolean(properties.getProperty("useCanonicalHostname", "false"));
        String host = address.getHost();
        if (canonical) {
            try {
                host = address.getInetAddress().getCanonicalHostName();
            } catch (java.net.UnknownHostException e) {
                // keep the host as given
            }
        }
        String realm = properties.getProperty("realm");
        return prefix + host + (realm == null ? "" : "@" + realm);
    }

    private static String rootMessage(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause instanceof GSSException gss ? gss.getMajorString() + " " + gss.getMinorString() : cause.getMessage();
    }

    @Override
    public void destroy() {
        // nothing to release
    }
}
