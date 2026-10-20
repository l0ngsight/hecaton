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

import uk.org.hecaton.config.SSLConfig;
import uk.org.hecaton.internal.nio.ClassLoaderUtil;
import uk.org.hecaton.nio.ssl.BasicSSLContextFactory;
import uk.org.hecaton.nio.ssl.SSLContextFactory;
import uk.org.hecaton.nio.ssl.SSLEngineFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;

/**
 * Creates the {@link SSLEngineFactory} that an {@link SSLConfig} describes.
 */
public final class SSLEngineFactories {

    /** SSL property: seconds a connection may take to complete the TLS handshake (default 10). */
    public static final String HANDSHAKE_TIMEOUT_SECONDS = "handshakeTimeoutSeconds";

    private static final int DEFAULT_HANDSHAKE_TIMEOUT_SECONDS = 10;

    private SSLEngineFactories() {
    }

    /** The handshake timeout that {@code config} sets, in milliseconds. */
    public static long handshakeTimeoutMillis(SSLConfig config) {
        String value = config.getProperty(HANDSHAKE_TIMEOUT_SECONDS);
        int seconds = value == null || value.isBlank() ? DEFAULT_HANDSHAKE_TIMEOUT_SECONDS : Integer.parseInt(value.trim());
        return TimeUnit.SECONDS.toMillis(seconds);
    }

    /**
     * @param forClient whether the factory is for a client (or for a member)
     * @param classLoader where to find the factory class named by the configuration
     * @return an initialized factory
     * @throws IllegalArgumentException if the configured factory is neither an
     *         {@link SSLEngineFactory} nor an {@link SSLContextFactory}
     */
    public static SSLEngineFactory create(SSLConfig config, boolean forClient, ClassLoader classLoader) {
        Object factory = config.getFactoryImplementation();
        try {
            if (factory == null) {
                String className = config.getFactoryClassName();
                factory = className == null || className.isBlank()
                        ? new BasicSSLContextFactory()
                        : ClassLoaderUtil.newInstance(classLoader, className);
            }
            SSLEngineFactory engineFactory;
            if (factory instanceof SSLEngineFactory f) {
                engineFactory = f;
            } else if (factory instanceof SSLContextFactory f) {
                engineFactory = new SSLContextEngineFactory(f);
            } else {
                throw new IllegalArgumentException("The SSL factory " + factory.getClass().getName()
                        + " is neither an SSLEngineFactory nor an SSLContextFactory");
            }
            Properties properties = config.getProperties();
            engineFactory.init(properties == null ? new Properties() : properties, forClient);
            return engineFactory;
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
