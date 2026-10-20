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

import uk.org.hecaton.config.SecurityConfig;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.security.CertificatesCallback;
import uk.org.hecaton.security.ClusterNameCallback;
import uk.org.hecaton.security.Credentials;
import uk.org.hecaton.security.CredentialsCallback;
import uk.org.hecaton.security.EndpointCallback;
import uk.org.hecaton.security.HecatonInstanceCallback;
import uk.org.hecaton.security.LoggingServiceCallback;
import uk.org.hecaton.security.RealmConfigCallback;
import uk.org.hecaton.security.SecurityContextCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.net.InetAddress;
import java.security.cert.Certificate;
import java.util.function.Supplier;

/**
 * Answers the callbacks of login modules for one login attempt.
 */
final class SecurityCallbackHandler implements CallbackHandler {

    private final Node node;
    private final SecurityContextImpl securityContext;
    private final String clusterName;
    private final Credentials credentials;
    private final InetAddress remoteAddress;
    private final Supplier<Certificate[]> certificates;

    SecurityCallbackHandler(Node node, SecurityContextImpl securityContext, String clusterName, Credentials credentials,
                            InetAddress remoteAddress, Supplier<Certificate[]> certificates) {
        this.node = node;
        this.securityContext = securityContext;
        this.clusterName = clusterName;
        this.credentials = credentials;
        this.remoteAddress = remoteAddress;
        this.certificates = certificates;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        SecurityConfig securityConfig = node.getConfig().getSecurityConfig();
        for (Callback callback : callbacks) {
            if (callback instanceof CredentialsCallback c) {
                c.setCredentials(credentials);
            } else if (callback instanceof EndpointCallback c) {
                c.setEndpoint(remoteAddress == null ? null : remoteAddress.getHostAddress());
            } else if (callback instanceof CertificatesCallback c) {
                c.setCertificates(certificates.get());
            } else if (callback instanceof ClusterNameCallback c) {
                c.setClusterName(clusterName);
            } else if (callback instanceof MemberIdentityCallback c) {
                c.setCredentials(securityContext.memberIdentity());
            } else if (callback instanceof RealmConfigCallback c) {
                c.setRealmConfig(securityConfig.getRealmConfig(c.getRealmName()));
            } else if (callback instanceof HecatonInstanceCallback c) {
                c.setHecatonInstance(node.hecatonInstance);
            } else if (callback instanceof LoggingServiceCallback c) {
                c.setLoggingService(node.getLoggingService());
            } else if (callback instanceof SecurityContextCallback c) {
                c.setSecurityContext(securityContext);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }
}
