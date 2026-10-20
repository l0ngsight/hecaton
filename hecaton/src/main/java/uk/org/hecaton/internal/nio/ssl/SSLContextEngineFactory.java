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
package uk.org.hecaton.internal.nio.ssl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.nio.ssl.BasicSSLContextFactory;
import uk.org.hecaton.nio.ssl.SSLContextFactory;
import uk.org.hecaton.nio.ssl.SSLEngineFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

/**
 * An {@link SSLEngineFactory} on an {@link SSLContextFactory}. Besides the context factory's
 * properties it reads, for every engine:
 * <ul>
 *     <li>{@code ciphersuites}, {@code protocols} – comma-separated cipher suites and protocol
 *     versions to enable (default: the JDK's);</li>
 *     <li>{@code mutualAuthentication} – {@code REQUIRED} or {@code OPTIONAL}: the accepting side
 *     asks the connecting side for a certificate (default: it does not);</li>
 *     <li>{@code validateIdentity} – {@code true}: the connecting side checks that the
 *     certificate matches the host name or address it connects to.</li>
 * </ul>
 */
public class SSLContextEngineFactory implements SSLEngineFactory {

    private final SSLContextFactory contextFactory;
    private SSLContext context;
    private String[] cipherSuites;
    private String[] protocols;
    private MutualAuthentication mutualAuthentication;
    private boolean validateIdentity;

    /** Whether the accepting side asks for the connecting side's certificate. */
    enum MutualAuthentication {
        NONE, OPTIONAL, REQUIRED
    }

    public SSLContextEngineFactory(SSLContextFactory contextFactory) {
        this.contextFactory = contextFactory;
    }

    @Override
    public void init(Properties properties, boolean forClient) throws Exception {
        if (!forClient && contextFactory instanceof BasicSSLContextFactory && properties.getProperty("keyStore") == null) {
            throw new IllegalStateException("TLS on a member needs a certificate: set the keyStore property of the SSL "
                    + "configuration (and keyStorePassword, keyStoreType)");
        }
        contextFactory.init(properties);
        context = contextFactory.getSSLContext();
        if (context == null) {
            throw new IllegalStateException(contextFactory.getClass().getName() + " returned no SSLContext");
        }
        cipherSuites = list(properties.getProperty("ciphersuites"));
        protocols = list(properties.getProperty("protocols"));
        String mutual = properties.getProperty("mutualAuthentication");
        mutualAuthentication = mutual == null || mutual.isBlank()
                ? MutualAuthentication.NONE
                : MutualAuthentication.valueOf(mutual.trim().toUpperCase(Locale.ROOT));
        validateIdentity = Boolean.parseBoolean(properties.getProperty("validateIdentity", "false"));
    }

    @Override
    public SSLEngine create(boolean clientMode, Address peerAddress) {
        SSLEngine engine = peerAddress == null
                ? context.createSSLEngine()
                : context.createSSLEngine(peerAddress.getHost(), peerAddress.getPort());
        engine.setUseClientMode(clientMode);
        if (cipherSuites != null) {
            engine.setEnabledCipherSuites(cipherSuites);
        }
        if (protocols != null) {
            engine.setEnabledProtocols(protocols);
        }
        if (clientMode) {
            if (validateIdentity) {
                SSLParameters parameters = engine.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                engine.setSSLParameters(parameters);
            }
        } else if (mutualAuthentication == MutualAuthentication.REQUIRED) {
            engine.setNeedClientAuth(true);
        } else if (mutualAuthentication == MutualAuthentication.OPTIONAL) {
            engine.setWantClientAuth(true);
        }
        return engine;
    }

    private static String[] list(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
    }
}
