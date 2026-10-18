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

package classloading;

import uk.org.hecaton.internal.util.FilteringClassLoader;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;

import static classloading.ThreadLocalLeakTestUtils.checkThreadLocalsForLeaks;
import static uk.org.hecaton.internal.util.RootCauseMatcher.rootCause;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Creates a member or client {@link uk.org.hecaton.core.Hecaton} instance with an explicit exclusion of {@code javax.cache}.
 * <p>
 * If the method {@link #createHecatonInstance()} or {@link #createHecatonInstance_getCacheManager()} fails with a
 * {@link ClassNotFoundException} with the cause "javax.cache.* - Package excluded explicitly!" we accidentally introduced
 * a runtime dependency on {@link javax.cache} with a default configuration.
 * <p>
 * The method {@link #createHecatonInstance_getCache()} is expected to fail, since it actually tries to invoke
 * {@link uk.org.hecaton.core.ICacheManager#getCache(String)}.
 */
public abstract class AbstractJavaXCacheDependencyTest {

    private static final String EXPECTED_CAUSE = "javax.cache.Cache - Package excluded explicitly!";

    private static final ClassLoader CLASS_LOADER;

    static {
        List<String> excludes = singletonList("javax.cache");
        CLASS_LOADER = new FilteringClassLoader(excludes, "uk.org.hecaton");
    }


    @Test
    public void createHecatonInstance() throws Exception {
        createHecatonInstance(false, false);

        checkThreadLocalsForLeaks(CLASS_LOADER);
    }

    @Test
    public void createHecatonInstance_getCacheManager() throws Exception {
        createHecatonInstance(true, false);

        checkThreadLocalsForLeaks(CLASS_LOADER);
    }

    @Test
    public void createHecatonInstance_getCache() throws Exception {
        createHecatonInstance(true, true);

        checkThreadLocalsForLeaks(CLASS_LOADER);
    }

    protected abstract String getConfigClass();

    protected abstract String getHecatonClass();

    protected abstract String getNewInstanceMethod();

    private void createHecatonInstance(boolean testGetCacheManager, boolean testGetCache) throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        thread.setContextClassLoader(CLASS_LOADER);
        try {
            Class<?> configClazz = CLASS_LOADER.loadClass(getConfigClass());
            Class<?> hecatonClazz = CLASS_LOADER.loadClass(getHecatonClass());
            Class<?> hecatonInstanceClazz = CLASS_LOADER.loadClass("uk.org.hecaton.core.HecatonInstance");
            Class<?> cacheManagerClazz = CLASS_LOADER.loadClass("uk.org.hecaton.core.ICacheManager");
            try {
                Object config = configClazz.newInstance();

                Method setClassLoader = configClazz.getDeclaredMethod("setClassLoader", ClassLoader.class);
                setClassLoader.invoke(config, CLASS_LOADER);

                Method newHecatonInstance = hecatonClazz.getDeclaredMethod(getNewInstanceMethod(), configClazz);
                Object hecatonInstance = newHecatonInstance.invoke(hecatonClazz, config);

                if (testGetCacheManager) {
                    Method getCacheManager = hecatonInstanceClazz.getDeclaredMethod("getCacheManager");
                    getCacheManager.invoke(hecatonInstance);
                }
                if (testGetCache) {
                    assertThatThrownBy(() -> cacheManagerClazz.getDeclaredMethod("getCache", String.class))
                            .has(rootCause(ClassNotFoundException.class, EXPECTED_CAUSE));
                }
            } finally {
                Method shutdownAll = hecatonClazz.getDeclaredMethod("shutdownAll");
                shutdownAll.invoke(hecatonClazz);
            }
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }
}
