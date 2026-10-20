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
package uk.org.hecaton.nio.ssl;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Properties;

/**
 * The default {@link SSLContextFactory}: builds an {@link SSLContext} from key and trust stores
 * on the file system.
 * <p>
 * Properties (all optional):
 * <ul>
 *     <li>{@code keyStore}, {@code keyStorePassword}, {@code keyStoreType} (default {@code JKS},
 *     which also reads PKCS#12), {@code keyManagerAlgorithm} (default: the JDK's) – the key and
 *     certificate this side presents. Without a key store this side has no certificate, which is
 *     fine for a client unless the member requires mutual authentication.</li>
 *     <li>{@code trustStore}, {@code trustStorePassword}, {@code trustStoreType} (default
 *     {@code JKS}), {@code trustManagerAlgorithm} (default: the JDK's) – the certificates this
 *     side trusts. Without a trust store, the JDK's default trust store is used.</li>
 *     <li>{@code protocol} (default {@code TLS}) – the {@link SSLContext} protocol.</li>
 *     <li>{@code keyMaterialDuration} – an ISO-8601 duration such as {@code PT1H}: the stores
 *     are read again when it has passed since they were last read, so renewed certificates are
 *     used for new connections without a restart. By default they are read once.</li>
 * </ul>
 * Cipher suites, enabled protocols, mutual authentication and hostname validation are applied
 * to each connection; see {@link uk.org.hecaton.config.SSLConfig}.
 */
public class BasicSSLContextFactory implements SSLContextFactory {

    /** Default key and trust store type. */
    public static final String DEFAULT_STORE_TYPE = "JKS";

    private Properties properties;
    private Duration keyMaterialDuration;
    private volatile SSLContext sslContext;
    private volatile long loadedAtNanos;

    @Override
    public void init(Properties properties) throws Exception {
        this.properties = properties;
        String duration = properties.getProperty("keyMaterialDuration");
        keyMaterialDuration = duration == null || duration.isBlank() ? null : Duration.parse(duration.trim());
        load();
    }

    @Override
    public SSLContext getSSLContext() {
        if (keyMaterialDuration != null && System.nanoTime() - loadedAtNanos > keyMaterialDuration.toNanos()) {
            synchronized (this) {
                if (System.nanoTime() - loadedAtNanos > keyMaterialDuration.toNanos()) {
                    try {
                        load();
                    } catch (Exception e) {
                        // keep the current key material; try again after another period
                        loadedAtNanos = System.nanoTime();
                        throw new IllegalStateException("Reading the key or trust store failed", e);
                    }
                }
            }
        }
        return sslContext;
    }

    private void load() throws GeneralSecurityException, IOException {
        KeyManager[] keyManagers = keyManagers(properties);
        TrustManager[] trustManagers = trustManagers(properties);
        SSLContext context = SSLContext.getInstance(properties.getProperty("protocol", "TLS"));
        context.init(keyManagers, trustManagers, null);
        sslContext = context;
        loadedAtNanos = System.nanoTime();
    }

    private static KeyManager[] keyManagers(Properties properties) throws GeneralSecurityException, IOException {
        String path = properties.getProperty("keyStore");
        if (path == null) {
            return null;
        }
        char[] password = password(properties, "keyStorePassword");
        KeyStore keyStore = load(path, properties.getProperty("keyStoreType", DEFAULT_STORE_TYPE), password);
        KeyManagerFactory factory = KeyManagerFactory.getInstance(
                properties.getProperty("keyManagerAlgorithm", KeyManagerFactory.getDefaultAlgorithm()));
        factory.init(keyStore, password);
        return factory.getKeyManagers();
    }

    private static TrustManager[] trustManagers(Properties properties) throws GeneralSecurityException, IOException {
        String path = properties.getProperty("trustStore");
        if (path == null) {
            return null;
        }
        KeyStore trustStore = load(path, properties.getProperty("trustStoreType", DEFAULT_STORE_TYPE),
                password(properties, "trustStorePassword"));
        TrustManagerFactory factory = TrustManagerFactory.getInstance(
                properties.getProperty("trustManagerAlgorithm", TrustManagerFactory.getDefaultAlgorithm()));
        factory.init(trustStore);
        return factory.getTrustManagers();
    }

    private static KeyStore load(String path, String type, char[] password) throws GeneralSecurityException, IOException {
        KeyStore store = KeyStore.getInstance(type);
        try (InputStream in = Files.newInputStream(Path.of(path))) {
            store.load(in, password);
        }
        return store;
    }

    private static char[] password(Properties properties, String name) {
        String password = properties.getProperty(name);
        return password == null ? null : password.toCharArray();
    }
}
