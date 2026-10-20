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

package uk.org.hecaton.test.starter;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.TestUtil;
import uk.org.hecaton.test.starter.answer.HecatonInstanceImplAnswer;
import uk.org.hecaton.test.starter.answer.NodeAnswer;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.internal.util.UuidUtil.newUnsecureUuidString;
import static uk.org.hecaton.test.starter.HecatonProxyFactory.proxyObjectForStarter;
import static uk.org.hecaton.test.starter.HecatonStarterUtils.debug;
import static uk.org.hecaton.test.starter.HecatonStarterUtils.rethrowGuardianException;
import static uk.org.hecaton.test.starter.ReflectionUtils.getFieldValueReflectively;
import static uk.org.hecaton.test.starter.ReflectionUtils.setFieldValueReflectively;
import static java.lang.Thread.currentThread;
import static java.lang.reflect.Proxy.isProxyClass;
import static org.mockito.Mockito.mock;

public class HecatonStarter {

    /**
     * Caches downloaded files and classloader used to load their classes per version string.
     */
    private static final ConcurrentMap<String, HecatonVersionClassloaderFuture> LOADED_VERSIONS =
            new ConcurrentHashMap<>();

    /**
     * Starts a new open source {@link HecatonInstance} of the given version.
     *
     * @param version the version string e.g. "3.8". Must correspond to a released version available either in the
     *                local maven repository or maven central.
     * @return a {@link HecatonInstance} proxying the started Hecaton instance.
     */
    public static HecatonInstance newHecatonInstance(String version) {
        return newHecatonInstance(version, null, false);
    }

    /**
     * Starts a new open source or enterprise edition {@link HecatonInstance} of the given version. Since no
     * explicit configuration is provided, a license key must be previously set as a system property to start
     * Hecaton enterprise edition.
     *
     * @param version    the version string e.g. "3.8". Must correspond to a released version available either in
     *                   local maven repository or maven central or Hecaton enterprise repository, in case
     *                   {@code enterprise} is true.
     * @param enterprise when {@code true}, start Hecaton enterprise edition, otherwise open source
     * @return a {@link HecatonInstance} proxying the started Hecaton instance.
     */
    public static HecatonInstance newHecatonInstance(String version, boolean enterprise) {
        return newHecatonInstance(version, null, enterprise);
    }

    /**
     * Starts a new {@link HecatonInstance} of the given {@code version} configured with the given {@code Config},
     * open source or enterprise edition.
     *
     * @param version        the version string e.g. "3.8". Must correspond to a released version available either in
     *                       local maven repository or maven central or Hecaton enterprise repository, in case
     *                       {@code enterprise} is {@code true}
     * @param configTemplate configuration object to clone on the target HecatonInstance. If {@code null}, default
     *                       configuration is assumed
     * @param enterprise     when {@code true}, start Hecaton enterprise edition, otherwise open source
     * @return a {@link HecatonInstance} proxying the started Hecaton instance.
     */
    public static HecatonInstance newHecatonInstance(String version, Config configTemplate, boolean enterprise) {
        return newHecatonInstance(version, configTemplate, enterprise, Collections.emptyList());
    }

    /**
     * Starts a new {@link HecatonInstance} of the given {@code version} configured with the given {@code Config},
     * open source or enterprise edition.
     *
     * @param version        the version string e.g. "3.8". Must correspond to a released version available either in
     *                       local maven repository or maven central or Hecaton enterprise repository, in case
     *                       {@code enterprise} is {@code true}
     * @param configTemplate configuration object to clone on the target HecatonInstance. If {@code null}, default
     *                       configuration is assumed
     * @param enterprise     when {@code true}, start Hecaton enterprise edition, otherwise open source
     * @param additionalJars additional jars to be loaded for this instance
     * @return a {@link HecatonInstance} proxying the started Hecaton instance.
     */
    public static HecatonInstance newHecatonInstance(String version, Config configTemplate, boolean enterprise,
                                                         List<URL> additionalJars) {
        ClassLoader cfgClassLoader = configTemplate == null ? null : configTemplate.getClassLoader();
        HecatonAPIDelegatingClassloader versionClassLoader = getTargetVersionClassloader(version, enterprise,
                cfgClassLoader, additionalJars);
        if (configTemplate != null) {
            configTemplate.getUserContext().put("versionClassLoader", versionClassLoader);
        }

        ClassLoader contextClassLoader = currentThread().getContextClassLoader();
        currentThread().setContextClassLoader(null);
        try {
            return newHecatonInstanceWithNetwork(versionClassLoader, configTemplate);
        } finally {
            if (contextClassLoader != null) {
                currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    public static HecatonInstance newHecatonInstance(Config configTemplate, HecatonAPIDelegatingClassloader classLoader) {
        if (configTemplate != null) {
            configTemplate.getUserContext().put("versionClassLoader", classLoader);
        }

        ClassLoader contextClassLoader = currentThread().getContextClassLoader();
        currentThread().setContextClassLoader(null);
        try {
            return newHecatonInstanceWithNetwork(classLoader, configTemplate);
        } finally {
            if (contextClassLoader != null) {
                currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    public static Node getNode(HecatonInstance hz) {
        try {
            HecatonAPIDelegatingClassloader classloader = getHecatonAPIDelegatingClassloader(hz);
            Object instance = getHecatonInstanceImpl(hz, classloader);
            Object node = getFieldValueReflectively(instance, "node");
            return mock(Node.class, new NodeAnswer(node));
        } catch (HecatonInstanceNotActiveException e) {
            throw new IllegalArgumentException("The given HecatonInstance is not an active HecatonInstanceImpl: "
                    + hz.getClass());
        } catch (Exception e) {
            throw rethrowGuardianException(e);
        }
    }

    public static HecatonInstanceImpl getHecatonInstanceImpl(HecatonInstance hz) {
        try {
            HecatonAPIDelegatingClassloader classloader = getHecatonAPIDelegatingClassloader(hz);
            Object instance = getHecatonInstanceImpl(hz, classloader);
            Object node = getFieldValueReflectively(instance, "node");
            HecatonInstanceImpl proxy = mock(HecatonInstanceImpl.class, new HecatonInstanceImplAnswer(instance));
            setFieldValueReflectively(proxy, "node", mock(Node.class, new NodeAnswer(node)));
            return proxy;
        } catch (HecatonInstanceNotActiveException e) {
            throw new IllegalArgumentException("The given HecatonInstance is not an active HecatonInstanceImpl: "
                    + hz.getClass());
        } catch (Exception e) {
            throw rethrowGuardianException(e);
        }
    }

    static HecatonAPIDelegatingClassloader getHecatonAPIDelegatingClassloader(HecatonInstance hz) {
        ConcurrentMap<String, Object> userContext = hz.getConfig().getUserContext();
        HecatonAPIDelegatingClassloader classloader
                = (HecatonAPIDelegatingClassloader) userContext.get("versionClassLoader");
        if (classloader == null) {
            throw new GuardianException("HecatonInstance was not started via HecatonStarter or without config template!");
        }
        return classloader;
    }

    static Object getHecatonInstanceImpl(HecatonInstance hecatonInstance, ClassLoader classloader)
            throws Exception {
        if (isProxyClass(hecatonInstance.getClass())) {
            InvocationHandler invocationHandler = Proxy.getInvocationHandler(hecatonInstance);
            Object delegate = getFieldValueReflectively(invocationHandler, "delegate");
            Class<?> instanceProxyClass = classloader.loadClass("uk.org.hecaton.instance.impl.HecatonInstanceProxy");
            Class<?> instanceImplClass = classloader.loadClass("uk.org.hecaton.instance.impl.HecatonInstanceImpl");
            if (instanceProxyClass.isAssignableFrom(delegate.getClass())) {
                Object instanceProxy = instanceProxyClass.cast(delegate);
                return instanceImplClass.cast(getFieldValueReflectively(instanceProxy, "original"));
            } else if (instanceImplClass.isAssignableFrom(delegate.getClass())) {
                return instanceImplClass.cast(delegate);
            }
            return delegate;
        }
        return TestUtil.getHecatonInstanceImpl(hecatonInstance);
    }

    /**
     * Obtains a {@link HecatonAPIDelegatingClassloader} with the given version's binaries in its classpath.
     * Classloaders are cached, so requesting the classloader for a given version multiple times will return the
     * same instance.
     *
     * @param version           the target Hecaton version e.g. "3.8.1", must be a published release version
     * @param configClassLoader class loader given via config
     * @return a classloader with given version's artifacts in its classpath
     */
    public static HecatonAPIDelegatingClassloader getTargetVersionClassloader(String version, boolean enterprise,
                                                                                ClassLoader configClassLoader) {
        return getTargetVersionClassloader(version, enterprise, configClassLoader, Collections.emptyList());
    }

    /**
     * Obtains a {@link HecatonAPIDelegatingClassloader} with the given version's binaries in its classpath.
     * Classloaders are cached, so requesting the classloader for a given version multiple times will return the
     * same instance.
     *
     * @param version           the target Hecaton version e.g. "3.8.1", must be a published release version
     * @param configClassLoader class loader given via config
     * @param additionalJars additional jars to be loaded for this instance
     * @return a classloader with given version's artifacts in its classpath
     */
    public static HecatonAPIDelegatingClassloader getTargetVersionClassloader(String version, boolean enterprise,
                                                                                ClassLoader configClassLoader,
                                                                                List<URL> additionalJars) {
        HecatonVersionClassloaderFuture future;
        if (configClassLoader != null) {
            // when a custom ClassLoader should be the parent of the target version ClassLoader,
            // do not use the ClassLoader cache
            future = new HecatonVersionClassloaderFuture(version, enterprise, configClassLoader, additionalJars);
            return future.get();
        }

        String versionSpec = versionSpec(version, enterprise);
        future = LOADED_VERSIONS.get(versionSpec);
        if (future != null) {
            return future.get();
        }

        future = new HecatonVersionClassloaderFuture(version, enterprise, null, additionalJars);
        HecatonVersionClassloaderFuture found = LOADED_VERSIONS.putIfAbsent(versionSpec, future);
        if (found != null) {
            HecatonAPIDelegatingClassloader versionClassLoader = found.get();
            if (versionClassLoader != null) {
                return versionClassLoader;
            }
        }

        try {
            return future.get();
        } catch (Throwable t) {
            LOADED_VERSIONS.remove(versionSpec, future);
            throw rethrow(t);
        }
    }

    private static HecatonInstance newHecatonInstanceWithNetwork(HecatonAPIDelegatingClassloader classloader,
                                                                     Config configTemplate) {
        try {
            Object delegate = createInstanceViaInstanceFactory(classloader, configTemplate);
            if (delegate == null) {
                delegate = createInstanceViaHecaton(classloader, configTemplate);
            }
            return (HecatonInstance) proxyObjectForStarter(HecatonStarter.class.getClassLoader(), delegate);
        } catch (ClassNotFoundException e) {
            throw rethrowGuardianException(e);
        }
    }

    private static Object createInstanceViaInstanceFactory(HecatonAPIDelegatingClassloader classloader, Config configTemplate) {
        try {
            Class<?> configClass = classloader.loadClass("uk.org.hecaton.config.Config");
            Object config = getConfig(classloader, configClass, configTemplate);
            String instanceName = configTemplate == null ? newUnsecureUuidString() : configTemplate.getInstanceName();

            Class<?> nodeContextClass = classloader.loadClass("uk.org.hecaton.instance.impl.NodeContext");
            Class<?> firewallingNodeContextClass = classloader.loadClass("uk.org.hecaton.instance.FirewallingNodeContext");
            Object nodeContext = proxyObjectForStarter(classloader, firewallingNodeContextClass.newInstance());

            Class<?> hecatonInstanceFactoryClass = classloader.loadClass("uk.org.hecaton.instance.impl.HecatonInstanceFactory");
            System.out.println(hecatonInstanceFactoryClass + " loaded by " + hecatonInstanceFactoryClass.getClassLoader());

            Method newHecatonInstanceMethod = hecatonInstanceFactoryClass.getMethod("newHecatonInstance", configClass,
                    String.class, nodeContextClass);
            return newHecatonInstanceMethod.invoke(null, config, instanceName, nodeContext);
        } catch (ClassNotFoundException | IllegalAccessException | NoSuchMethodException | InvocationTargetException
                 | InstantiationException e) {
            debug("Could not create HecatonInstance via HecatonInstanceFactory: " + e.getMessage());
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Object createInstanceViaHecaton(HecatonAPIDelegatingClassloader classloader, Config configTemplate) {
        try {
            Class<?> configClass = classloader.loadClass("uk.org.hecaton.config.Config");
            Object config = getConfig(classloader, configClass, configTemplate);

            Class<Hecaton> hecatonClass = (Class<Hecaton>) classloader.loadClass("uk.org.hecaton.core.Hecaton");
            System.out.println(hecatonClass + " loaded by " + hecatonClass.getClassLoader());

            Method newHecatonInstanceMethod = hecatonClass.getMethod("newHecatonInstance", configClass);
            return newHecatonInstanceMethod.invoke(null, config);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException
                 | InstantiationException e) {
            throw rethrowGuardianException(e);
        }
    }

    public static Object getConfig(HecatonAPIDelegatingClassloader classloader, Class<?> configClass, Object configTemplate)
            throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException,
            ClassNotFoundException {
        Object config;
        if (configTemplate == null) {
            config = configClass.newInstance();
            Method setClassLoaderMethod = configClass.getMethod("setClassLoader", ClassLoader.class);
            setClassLoaderMethod.invoke(config, classloader);
        } else {
            config = proxyObjectForStarter(classloader, configTemplate);
        }
        return config;
    }

    private static String versionSpec(String version, boolean enterprise) {
        return enterprise ? version + "-EE" : version;
    }

    private static class HecatonVersionClassloaderFuture {

        private final String version;
        private final boolean enterprise;
        private final ClassLoader configClassLoader;
        private final List<URL> additionalJars = new ArrayList<>();

        private HecatonAPIDelegatingClassloader classLoader;

        HecatonVersionClassloaderFuture(String version, boolean enterprise, ClassLoader configClassLoader,
                                          List<URL> additionalJars) {
            this.version = version;
            this.enterprise = enterprise;
            this.configClassLoader = configClassLoader;
            this.additionalJars.addAll(additionalJars);
        }

        public HecatonAPIDelegatingClassloader get() {
            if (classLoader != null) {
                return classLoader;
            }
            synchronized (this) {
                Map<HecatonVersionLocator.Artifact, File> files =
                        HecatonVersionLocator.locateVersion(version, enterprise);
                List<URL> urls = fileIntoUrls(files);
                urls.addAll(additionalJars);
                ClassLoader parentClassloader = HecatonStarter.class.getClassLoader();
                if (configClassLoader != null) {
                    parentClassloader = configClassLoader;
                }
                classLoader = new HecatonAPIDelegatingClassloader(urls.toArray(new URL[]{}), parentClassloader);
                return classLoader;
            }
        }

        private static List<URL> fileIntoUrls(Map<HecatonVersionLocator.Artifact, File> files) {
            List<URL> urls = new ArrayList<>(files.size());
            for (File file : files.values()) {
                try {
                    urls.add(file.toURI().toURL());
                } catch (MalformedURLException e) {
                    throw rethrowGuardianException(e);
                }
            }
            return urls;
        }
    }
}
