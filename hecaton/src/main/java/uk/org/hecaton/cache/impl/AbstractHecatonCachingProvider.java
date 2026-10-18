/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.cache.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.StringUtil;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;

import javax.annotation.Nonnull;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;

import static uk.org.hecaton.cache.HecatonCachingProvider.HECATON_CONFIG_LOCATION;
import static uk.org.hecaton.cache.HecatonCachingProvider.HECATON_INSTANCE_ITSELF;
import static uk.org.hecaton.cache.HecatonCachingProvider.HECATON_INSTANCE_NAME;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getProperty;

/**
 * Abstract {@link CachingProvider} implementation providing shared
 * functionality to server and client caching providers.
 * <p>
 * This class encapsulates following details:
 * <ul>
 * <li>Hecaton instance for the default URI and default classloader</li>
 * <li>the default URI</li>
 * <li>the default classloader</li>
 * <li>accessing the singleton {@link CacheManager} by URI and classloader</li>
 * <li>managing lifecycle of cache managers</li>
 * </ul>
 *
 * @see CachingProvider
 */
public abstract class AbstractHecatonCachingProvider implements CachingProvider {

    /**
     * System property to control whether the default Hecaton instance, which
     * may be started when obtaining the default {@link CachingProvider}, will
     * have an instance name set or not.
     * <p>
     * When not set or when system property {@code hecaton.named.jcache.instance}
     * is {@code true}, then a common instance name is used to get-or-create
     * the default {@link HecatonInstance}. When the system property
     * {@code hecaton.named.jcache.instance} is {@code false}, then no
     * instance name is set on the default configuration.
     */
    public static final String NAMED_JCACHE_HZ_INSTANCE = "hecaton.named.jcache.instance";

    protected static final ILogger LOGGER = Logger.getLogger(AbstractHecatonCachingProvider.class);

    private static final String INVALID_HZ_INSTANCE_SPECIFICATION_MESSAGE = "No available Hecaton instance."
            + " Please specify your Hecaton configuration file path via"
            + " \"HecatonCachingProvider.HECATON_CONFIG_LOCATION\" property or specify Hecaton instance name via"
            + " \"HecatonCachingProvider.HECATON_INSTANCE_NAME\" property in the \"properties\" parameter.";

    private static final Set<String> SUPPORTED_SCHEMES;

    static {
        SUPPORTED_SCHEMES = Set.of("classpath", "file", "http", "https", "jar");
    }

    protected final boolean namedDefaultHzInstance = parseBoolean(getProperty(NAMED_JCACHE_HZ_INSTANCE, "true"));

    protected volatile HecatonInstance hecatonInstance;

    private final ClassLoader defaultClassLoader;
    private final URI defaultURI;

    private final Map<ClassLoader, Map<URI, AbstractHecatonCacheManager>> cacheManagers;

    protected AbstractHecatonCachingProvider() {
        // we use a WeakHashMap to prevent strong references to a classLoader to avoid memory leaks
        this.cacheManagers = new WeakHashMap<>();
        this.defaultClassLoader = getClass().getClassLoader();
        try {
            defaultURI = new URI("hecaton");
        } catch (URISyntaxException e) {
            throw new CacheException("Cannot create default URI", e);
        }
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        URI managerURI = getManagerUri(uri);
        ClassLoader managerClassLoader = getManagerClassLoader(classLoader);
        Properties managerProperties = properties == null ? new Properties() : properties;
        synchronized (cacheManagers) {
            Map<URI, AbstractHecatonCacheManager> cacheManagersByURI = cacheManagers.computeIfAbsent(managerClassLoader,
                    x -> new HashMap<>());
            AbstractHecatonCacheManager cacheManager = cacheManagersByURI.get(managerURI);
            if (cacheManager == null || cacheManager.isClosed()) {
                try {
                    cacheManager = createHecatonCacheManager(uri, classLoader, managerProperties);
                    cacheManagersByURI.put(managerURI, cacheManager);
                } catch (Exception e) {
                    throw new CacheException("Error opening URI [" + managerURI.toString() + ']', e);
                }
            }
            return cacheManager;
        }
    }

    @Override
    public ClassLoader getDefaultClassLoader() {
        return defaultClassLoader;
    }

    @Override
    public URI getDefaultURI() {
        return defaultURI;
    }

    @Override
    public Properties getDefaultProperties() {
        return null;
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return getCacheManager(uri, classLoader, null);
    }

    @Override
    public CacheManager getCacheManager() {
        return getCacheManager(null, null, null);
    }

    @Override
    public void close() {
        // closing a CachingProvider does not mean to close it forever, see JavaDoc of close()
        synchronized (cacheManagers) {
            for (Map<URI, AbstractHecatonCacheManager> cacheManagersByURI : cacheManagers.values()) {
                for (AbstractHecatonCacheManager cacheManager : cacheManagersByURI.values()) {
                    if (cacheManager.isDefaultClassLoader) {
                        cacheManager.close();
                    } else {
                        cacheManager.destroy();
                    }
                }
            }
        }
        this.cacheManagers.clear();
        shutdownHecatonInstance();
    }

    private void shutdownHecatonInstance() {
        HecatonInstance localInstanceRef = hecatonInstance;
        if (localInstanceRef != null) {
            localInstanceRef.shutdown();
        }
        hecatonInstance = null;
    }

    @Override
    public void close(ClassLoader classLoader) {
        ClassLoader managerClassLoader = getManagerClassLoader(classLoader);
        synchronized (cacheManagers) {
            Map<URI, AbstractHecatonCacheManager> cacheManagersByURI = this.cacheManagers.get(managerClassLoader);
            if (cacheManagersByURI != null) {
                for (CacheManager cacheManager : cacheManagersByURI.values()) {
                    cacheManager.close();
                }
            }
        }
    }

    @Override
    public void close(URI uri, ClassLoader classLoader) {
        URI managerURI = getManagerUri(uri);
        ClassLoader managerClassLoader = getManagerClassLoader(classLoader);
        synchronized (cacheManagers) {
            Map<URI, AbstractHecatonCacheManager> cacheManagersByURI = this.cacheManagers.get(managerClassLoader);
            if (cacheManagersByURI != null) {
                CacheManager cacheManager = cacheManagersByURI.remove(managerURI);
                if (cacheManager != null) {
                    cacheManager.close();
                }
                if (cacheManagersByURI.isEmpty()) {
                    cacheManagers.remove(classLoader);
                }
            }
        }
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        switch (optionalFeature) {
            // Hecaton is distributed only and does not have a local in-process mode,
            // therefore the optional store-by-reference mode is not supported
            case STORE_BY_REFERENCE:
                return false;
            default:
                return false;
        }
    }

    private URI getManagerUri(URI uri) {
        return uri == null ? defaultURI : uri;
    }

    private ClassLoader getManagerClassLoader(ClassLoader classLoader) {
        return classLoader == null ? defaultClassLoader : classLoader;
    }

    private <T extends AbstractHecatonCacheManager> T createHecatonCacheManager(URI uri, ClassLoader classLoader,
                                                                                    Properties managerProperties) {
        HecatonInstance instance;
        try {
            instance = getOrCreateInstance(uri, classLoader, managerProperties);
            if (instance == null) {
                throw new IllegalArgumentException(INVALID_HZ_INSTANCE_SPECIFICATION_MESSAGE);
            }
        } catch (Exception e) {
            throw rethrow(e);
        }
        return createCacheManager(instance, uri, classLoader, managerProperties);
    }

    protected URL getConfigURL(URI location, @Nonnull ClassLoader classLoader)
            throws URISyntaxException, IOException {
        String scheme = location.getScheme();
        if (scheme == null) {
            // interpret as place holder
            location = new URI(System.getProperty(location.getRawSchemeSpecificPart()));
            scheme = location.getScheme();
        }
        URL configURL;
        if ("classpath".equals(scheme)) {
            configURL = classLoader.getResource(location.getRawSchemeSpecificPart());
        } else if ("file".equals(scheme) || "http".equals(scheme) || "https".equals(scheme) || "jar".equals(scheme)) {
            configURL = location.toURL();
        } else {
            throw new URISyntaxException(location.toString(), "Unsupported protocol in configuration location URL");
        }
        return configURL;
    }

    /**
     * Get or create a {@code HecatonInstance} taking into account configuration
     * from {@code uri}, the given {@code classLoader} and {@code instanceName}.
     *
     * @param uri                   a {@link URI} where configuration is located. Supported
     *                              schemes are {@code http}, {@code https}, {@code file} and
     *                              {@code classpath}.
     * @param classLoader           the {@link ClassLoader} to resolve configuration resource
     *                              if given {@code uri} has {@code classpath} scheme. When {@code null},
     *                              the default class loader from {@link #getDefaultClassLoader()}
     *                              is used.
     * @param instanceName          the instance name to identify the {@code HecatonInstance}. When
     *                              not {@code null}, it will be used to override the instance name in the
     *                              resolved configuration.
     * @return                      a {@code HecatonInstance}
     * @throws URISyntaxException   when {@code uri} cannot be parsed
     * @throws IOException          when there is a failure retrieving configuration from the resource
     *                              indicated by {@code uri}
     */
    @Nonnull
    protected abstract HecatonInstance getOrCreateFromUri(@Nonnull URI uri,
                                                            ClassLoader classLoader,
                                                            String instanceName)
            throws URISyntaxException, IOException;

    /**
     * Gets an existing {@link HecatonInstance} by {@code instanceName} or,
     * if not found, creates a new {@link HecatonInstance} with the default
     * configuration and given {@code instanceName}.
     *
     * @param instanceName name to look up an existing {@link HecatonInstance}
     *                     or to create a new one
     * @return a {@link HecatonInstance} with the given {@code instanceName}
     */
    protected abstract HecatonInstance getOrCreateByInstanceName(String instanceName);

    @Nonnull
    protected abstract HecatonInstance getDefaultInstance();

    protected abstract <T extends AbstractHecatonCacheManager> T createCacheManager(HecatonInstance instance,
                                                                                      URI uri, ClassLoader classLoader,
                                                                                      Properties properties);

    private HecatonInstance getOrCreateInstance(URI uri, ClassLoader classLoader, Properties properties)
            throws URISyntaxException, IOException {
        HecatonInstance instance = getOrCreateInstanceFromProperties(classLoader, properties);
        if (instance != null) {
            return instance;
        }

        // resolving HecatonInstance via properties failed, try with URI as XML configuration file location
        String instanceName = properties.getProperty(HECATON_INSTANCE_NAME);
        boolean isDefaultURI = (uri == null || uri.equals(getDefaultURI()));
        if (!isDefaultURI && isConfigLocation(uri)) {
            // attempt to resolve URI as config location or as instance name
            try {
                return getOrCreateFromUri(uri, classLoader, instanceName);
            } catch (Exception e) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Could not get or create Hecaton instance from URI " + uri, e);
                }
            }
            // could not locate the Hecaton instance, return null and an exception will be thrown by the invoker
            return null;
        } else {
            return getDefaultInstance();
        }
    }

    private HecatonInstance getOrCreateInstanceFromProperties(ClassLoader classLoader, Properties properties)
            throws URISyntaxException, IOException {
        // if the Hecaton instance itself is specified via properties, return it
        HecatonInstance instanceItself = (HecatonInstance) properties.get(HECATON_INSTANCE_ITSELF);
        if (instanceItself != null) {
            return instanceItself;
        }

        // if the config location is specified, get the Hecaton instance through it
        String location = properties.getProperty(HECATON_CONFIG_LOCATION);
        String instanceName = properties.getProperty(HECATON_INSTANCE_NAME);
        if (location != null) {
            return getOrCreateFromUri(new URI(location), classLoader, instanceName);
        }

        // if instance name is specified, get the Hecaton instance through it
        if (instanceName != null) {
            return getOrCreateByInstanceName(instanceName);
        }

        // failed to locate or create HecatonInstance from properties
        return null;
    }

    // returns true when location itself or its resolved value as system property placeholder has one of supported schemes
    // from which Config objects can be initialized
    private boolean isConfigLocation(URI location) {
        String scheme = location.getScheme();
        if (scheme == null) {
            // interpret as place holder
            try {
                String resolvedPlaceholder = getProperty(location.getRawSchemeSpecificPart());
                if (resolvedPlaceholder == null) {
                    return false;
                }
                location = new URI(resolvedPlaceholder);
                scheme = location.getScheme();
            } catch (URISyntaxException e) {
                return false;
            }
        }
        return (scheme != null && SUPPORTED_SCHEMES.contains(scheme.toLowerCase(StringUtil.LOCALE_INTERNAL)));
    }

    /**
     * Tests if the configuration file provided is a YAML file. The test is based on the file extension.
     * @param configURL configuration URL to test.
     * @return true if the configuration file extension is .yml or .yaml; otherwise, false.
     */
    protected static boolean isYamlConfiguration(URL configURL) {
        String path = configURL.getPath();
        return path.endsWith(".yml") || path.endsWith(".yaml");
    }
}
