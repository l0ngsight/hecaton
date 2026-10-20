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

import uk.org.hecaton.security.ClusterLoginModule;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Locale;

/**
 * Authentication by the client certificate of a mutual TLS connection ({@code tls}
 * authentication): the identity is the certificate's subject (RFC 2253), and the values of the
 * subject attribute named by option {@code roleAttribute} (default {@code cn}) are its roles.
 * The certificate itself was verified in the TLS handshake against the member's trust store.
 */
public class X509CertificateLoginModule extends ClusterLoginModule {

    private String name;

    @Override
    protected boolean onLogin() throws LoginException {
        List<Certificate> certificates = getCertificates();
        if (certificates.isEmpty() || !(certificates.get(0) instanceof X509Certificate certificate)) {
            throw new FailedLoginException("No client certificate: TLS with mutual authentication is required");
        }
        name = certificate.getSubjectX500Principal().getName();
        String roleAttribute = getStringOption("roleAttribute", "cn").toLowerCase(Locale.ROOT);
        try {
            for (Rdn rdn : new LdapName(name).getRdns()) {
                if (rdn.getType().toLowerCase(Locale.ROOT).equals(roleAttribute)) {
                    addRole(rdn.getValue().toString());
                }
            }
        } catch (InvalidNameException e) {
            LoginException exception = new FailedLoginException("Invalid certificate subject " + name);
            exception.initCause(e);
            throw exception;
        }
        return true;
    }

    @Override
    protected String getName() {
        return name;
    }
}
