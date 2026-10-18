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

package uk.org.hecaton.cache.jsr;

import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.test.JmxLeakHelper;

import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import javax.management.ObjectInstance;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static uk.org.hecaton.test.HecatonTestSupport.assertThatIsNotMultithreadedTest;
import static java.lang.String.format;
import static org.junit.Assert.fail;

/**
 * Utility class responsible for setup/cleanup of JSR member tests.
 */
public final class JsrTestUtil {

    /**
     * Keeps track of system properties set by this utility.
     * <p>
     * We have to manage the System properties by ourselves, since they are set in {@link org.junit.BeforeClass} methods,
     * which are invoked before our Hecaton {@link org.junit.runner.Runner} classes are copying the System properties
     * to restore them for us.
     */
    private static final List<String> SYSTEM_PROPERTY_REGISTRY = new LinkedList<>();

    private JsrTestUtil() {
    }

    public static void setup() {
        assertThatIsNotMultithreadedTest();
        setSystemProperties("server");
        assertNoMBeanLeftovers();
    }

    public static void cleanup() {
        clearSystemProperties();
        clearCachingProviderRegistry();

        Hecaton.shutdownAll();
        HecatonInstanceFactory.terminateAll();
        assertNoMBeanLeftovers();
    }

    /**
     * Sets the System properties for JSR related tests including the JCache provider type.
     *
     * @param providerType "server" or "client" according to your test type
     */
    public static void setSystemProperties(String providerType) {
        /*
        If we don't set this parameter the HecatonCachingProvider will try to determine if it has to
        create a client or server CachingProvider by looking for the client class. If you run the testsuite
        from IDEA across all modules, that class is available (even though you might want to start a server
        side test). This leads to a ClassCastException for server side tests, since a client CachingProvider
        will be created. So we explicitly set this property to ease the test setups for IDEA environments.
         */
        setSystemProperty("hecaton.jcache.provider.type", providerType);

        setSystemProperties();
    }

    /**
     * Sets the System properties for JSR related tests.
     */
    public static void setSystemProperties() {
        // uses plain strings to avoid triggering any classloading of JSR classes with static code initializations
        setSystemProperty("javax.management.builder.initial", "uk.org.hecaton.cache.impl.TCKMBeanServerBuilder");
        setSystemProperty("CacheManagerImpl", "uk.org.hecaton.cache.HecatonCacheManager");
        setSystemProperty("javax.cache.Cache", "uk.org.hecaton.cache.ICache");
        setSystemProperty("javax.cache.Cache.Entry", "uk.org.hecaton.cache.impl.CacheEntry");
        setSystemProperty("javax.cache.annotation.CacheInvocationContext",
                "javax.cache.annotation.impl.cdi.CdiCacheKeyInvocationContextImpl");
    }

    /**
     * Clears the System properties for JSR related tests.
     */
    public static void clearSystemProperties() {
        for (String key : SYSTEM_PROPERTY_REGISTRY) {
            System.clearProperty(key);
        }
        SYSTEM_PROPERTY_REGISTRY.clear();
    }

    /**
     * Closes and removes the {@link javax.cache.spi.CachingProvider} from the static registry in {@link Caching}.
     */
    public static void clearCachingProviderRegistry() {
        try {
            // retrieve the CachingProviderRegistry instance
            Field providerRegistryField = getProviderRegistryField();

            // retrieve the map with the CachingProvider instances
            Map<ClassLoader, Map<String, CachingProvider>> providerMap = getProviderMap(providerRegistryField);

            // close all existing CachingProvider
            for (Map<String, CachingProvider> providers : providerMap.values()) {
                for (CachingProvider provider : providers.values()) {
                    try {
                        provider.close();
                    } catch (HecatonInstanceNotActiveException ignored) {
                        // this is fine, since the instances can already be stopped
                    }
                }
            }

            // clear the CachingProvider map
            providerMap.clear();

            Class<?> providerRegistryClass = providerRegistryField.getType();
            Object providerRegistryInstance = providerRegistryField.get(Caching.class);

            // retrieve the ClassLoader of the CachingProviderRegistry
            Field classLoaderField = providerRegistryClass.getDeclaredField("classLoader");
            classLoaderField.setAccessible(true);

            // set the ClassLoader to null
            classLoaderField.set(providerRegistryInstance, null);
        } catch (Exception e) {
            e.printStackTrace();
            fail(format("Could not cleanup CachingProvider registry: [%s] %s", e.getClass().getSimpleName(), e.getMessage()));
        }
    }

    /**
     * Returns the number of registered {@link javax.cache.spi.CachingProvider} from the static registry in {@link Caching}.
     */
    public static int getCachingProviderRegistrySize() {
        try {
            // retrieve the CachingProviderRegistry instance
            Field providerRegistryField = getProviderRegistryField();

            // retrieve the map with the CachingProvider instances
            Map<ClassLoader, Map<String, CachingProvider>> providerMap = getProviderMap(providerRegistryField);

            // count the number of existing CachingProviders
            int count = 0;
            for (Map<String, CachingProvider> providers : providerMap.values()) {
                count += providers.values().size();
            }

            // return the map size
            return count;
        } catch (NoClassDefFoundError | Exception e) {
            return -1;
        }
    }

    private static void setSystemProperty(String key, String value) {
        // we just want to set a System property, which has not been set already
        // this way you can always override a JSR setting manually
        if (System.getProperty(key) == null) {
            System.setProperty(key, value);
            SYSTEM_PROPERTY_REGISTRY.add(key);
        }
    }

    private static Field getProviderRegistryField() throws NoSuchFieldException {
        Field providerRegistryField = Caching.class.getDeclaredField("CACHING_PROVIDERS");
        providerRegistryField.setAccessible(true);

        return providerRegistryField;
    }

    private static Map<ClassLoader, Map<String, CachingProvider>> getProviderMap(Field providerRegistryField) throws Exception {
        Class<?> providerRegistryClass = providerRegistryField.getType();
        Object providerRegistryInstance = providerRegistryField.get(Caching.class);

        // retrieve the map with the CachingProvider instances
        Field providerMapField = providerRegistryClass.getDeclaredField("cachingProviders");
        providerMapField.setAccessible(true);

        //noinspection unchecked
        return (Map<ClassLoader, Map<String, CachingProvider>>) providerMapField.get(providerRegistryInstance);
    }

    public static void assertNoMBeanLeftovers() {
        Collection<ObjectInstance> leftovers = JmxLeakHelper.getActiveJmxBeansWithPrefix("javax.cache");

        if (leftovers.isEmpty()) {
            return;
        }
        fail("Leftover MBeans are still registered with the platform MBeanServer: "
                + leftovers);
    }
}
