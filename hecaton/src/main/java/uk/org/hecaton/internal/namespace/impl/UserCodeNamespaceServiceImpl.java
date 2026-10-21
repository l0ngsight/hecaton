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

package uk.org.hecaton.internal.namespace.impl;

import uk.org.hecaton.config.ConfigAccessor;
import uk.org.hecaton.config.UserCodeNamespaceConfig;
import uk.org.hecaton.config.UserCodeNamespacesConfig;
import uk.org.hecaton.internal.metrics.MetricDescriptor;
import uk.org.hecaton.internal.metrics.MetricsCollectionContext;
import uk.org.hecaton.internal.monitor.impl.LocalUserCodeNamespaceResourceStats;
import uk.org.hecaton.internal.monitor.impl.LocalUserCodeNamespaceResourceStatsImpl;
import uk.org.hecaton.internal.monitor.impl.LocalUserCodeNamespaceStats;
import uk.org.hecaton.internal.monitor.impl.LocalUserCodeNamespaceStatsImpl;
import uk.org.hecaton.internal.namespace.ResourceDefinition;
import uk.org.hecaton.internal.namespace.UserCodeNamespaceService;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.serialization.SerializationClassNameFilter;
import uk.org.hecaton.internal.util.ExceptionUtil;
import uk.org.hecaton.jet.impl.JobRepository;
import uk.org.hecaton.jet.impl.deployment.MapResourceClassLoader;
import uk.org.hecaton.function.ThrowingRunnable;
import uk.org.hecaton.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.USER_CODE_NAMESPACE_DISCRIMINATOR_NAME;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.USER_CODE_NAMESPACE_PREFIX;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.USER_CODE_NAMESPACE_RESOURCE_ID_TAG;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.USER_CODE_NAMESPACE_RESOURCE_PREFIX;
import static uk.org.hecaton.jet.impl.JobRepository.classKeyName;
import static uk.org.hecaton.jet.impl.util.ReflectionUtils.toClassResourceId;

/**
 * User Code Namespaces (F17): every namespace has a {@link MapResourceClassLoader} holding its
 * classes and resources (from classes, JARs and ZIPs of JARs), which looks in the namespace first
 * and then in the member's class loader. Code that runs for a data structure configured with a
 * namespace runs with that namespace's class loader on its thread (see {@link uk.org.hecaton.internal.namespace.NamespaceUtil}).
 * Adding a namespace with the name of an existing one replaces it for code that runs afterwards.
 */
public final class UserCodeNamespaceServiceImpl implements UserCodeNamespaceService {

    private static final String CLASS_SUFFIX = ".class";

    private final ClassLoader parent;
    /** The {@code class-filter}: which of the namespaces' classes may be loaded; null for all. */
    private final SerializationClassNameFilter classFilter;
    private final ILogger logger;
    private final Map<String, Namespace> namespaces = new ConcurrentHashMap<>();
    private volatile boolean defaultNamespaceDefined;

    /**
     * @param configClassLoader the member's class loader; when it is the {@link NamespaceAwareClassLoader},
     *                          the namespaces' parent is the loader it delegates to
     */
    public UserCodeNamespaceServiceImpl(ClassLoader configClassLoader, UserCodeNamespacesConfig config, ILogger logger) {
        this.parent = configClassLoader instanceof NamespaceAwareClassLoader namespaceAware
                ? namespaceAware.getParent() : configClassLoader;
        this.classFilter = config.getClassFilterConfig() == null ? null
                : new SerializationClassNameFilter(config.getClassFilterConfig());
        this.logger = logger;
        for (UserCodeNamespaceConfig namespaceConfig : ConfigAccessor.getNamespaceConfigs(config).values()) {
            addNamespace(namespaceConfig.getName(), ConfigAccessor.getResourceDefinitions(namespaceConfig));
        }
    }

    @Override
    public void addNamespace(@Nonnull String nsName, @Nonnull Collection<ResourceDefinition> resources) {
        Map<String, byte[]> content = new HashMap<>();
        Map<String, LocalUserCodeNamespaceResourceStats> resourceStats = new HashMap<>();
        for (ResourceDefinition resource : resources) {
            addResource(content, resource);
            resourceStats.put(resource.id(), new LocalUserCodeNamespaceResourceStatsImpl(
                    resource.payload() == null ? 0 : resource.payload().length, resource.type()));
        }
        Map<String, byte[]> immutable = Map.copyOf(content);
        MapResourceClassLoader classLoader = new NamespaceClassLoader(nsName, parent, immutable, classFilter);
        LocalUserCodeNamespaceStatsImpl stats = new LocalUserCodeNamespaceStatsImpl();
        stats.setLocalUserCodeNamespaceResourceStats(resourceStats);
        Namespace previous = namespaces.put(nsName, new Namespace(classLoader, stats));
        if (DEFAULT_NAMESPACE_NAME.equals(nsName)) {
            defaultNamespaceDefined = true;
        }
        logger.info((previous == null ? "Added" : "Replaced") + " the User Code Namespace " + nsName + " with "
                + resources.size() + " resource(s)");
    }

    private static void addResource(Map<String, byte[]> content, ResourceDefinition resource) {
        byte[] payload = resource.payload();
        if (payload == null) {
            throw new IllegalArgumentException("The resource " + resource.id() + " has no content");
        }
        try {
            switch (resource.type()) {
                case CLASS -> {
                    String id = resource.id().endsWith(CLASS_SUFFIX) ? resource.id() : toClassResourceId(resource.id());
                    content.put(classKeyName(id), IOUtil.compress(payload));
                }
                case JAR -> addJar(content, new ByteArrayInputStream(payload));
                case JARS_IN_ZIP -> JobRepository.executeOnJarsInZIP(new ByteArrayInputStream(payload), zip -> {
                    try {
                        addJar(content, zip);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                default -> throw new IllegalArgumentException("User Code Namespaces do not support resources of type "
                        + resource.type() + " (" + resource.id() + ")");
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read the resource " + resource.id(), e);
        }
    }

    /** Adds the entries of a JAR; the first of entries with the same name wins. */
    private static void addJar(Map<String, byte[]> content, InputStream jar) throws IOException {
        JarInputStream in = new JarInputStream(jar);
        JarEntry entry;
        while ((entry = in.getNextJarEntry()) != null) {
            if (!entry.isDirectory()) {
                content.putIfAbsent(classKeyName(entry.getName()), IOUtil.compress(in.readAllBytes()));
            }
        }
    }

    @Override
    public boolean hasNamespace(String namespace) {
        return namespace != null && namespaces.containsKey(namespace);
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public boolean isDefaultNamespaceDefined() {
        return defaultNamespaceDefined;
    }

    @Override
    public void setupNamespace(@Nullable String namespace) {
        ClassLoader classLoader = getClassLoaderForNamespace(namespace);
        if (classLoader != null) {
            NamespaceThreadLocalContext.onStartNsAware(classLoader);
        }
    }

    @Override
    public void cleanupNamespace(@Nullable String namespace) {
        String name = transformNamespace(namespace);
        if (name != null && namespaces.containsKey(name)) {
            // by name: the namespace may have been replaced since setupNamespace
            NamespaceThreadLocalContext.onCompleteNsAware(name);
        }
    }

    @Override
    public void runWithNamespace(@Nullable String namespace, ThrowingRunnable runnable) {
        setupNamespace(namespace);
        try {
            runnable.run();
        } finally {
            cleanupNamespace(namespace);
        }
    }

    @Override
    public <V> V callWithNamespace(@Nullable String namespace, Callable<V> callable) {
        setupNamespace(namespace);
        try {
            return callable.call();
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        } finally {
            cleanupNamespace(namespace);
        }
    }

    @Override
    public ClassLoader getClassLoaderForNamespace(String namespace) {
        String name = transformNamespace(namespace);
        Namespace ns = name == null ? null : namespaces.get(name);
        return ns == null ? null : ns.classLoader;
    }

    @Override
    public String transformNamespace(@Nullable String namespace) {
        if (namespace != null) {
            return namespace;
        }
        return defaultNamespaceDefined ? DEFAULT_NAMESPACE_NAME : null;
    }

    @Override
    public Map<String, LocalUserCodeNamespaceStats> getStats() {
        Map<String, LocalUserCodeNamespaceStats> stats = new HashMap<>();
        namespaces.forEach((name, ns) -> stats.put(name, ns.stats));
        return stats;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        namespaces.forEach((name, ns) -> {
            MetricDescriptor namespaceDescriptor = descriptor.copy().withPrefix(USER_CODE_NAMESPACE_PREFIX)
                    .withDiscriminator(USER_CODE_NAMESPACE_DISCRIMINATOR_NAME, name);
            context.collect(namespaceDescriptor, ns.stats);
            ns.stats.getResources().forEach((id, resource) -> context.collect(descriptor.copy()
                    .withPrefix(USER_CODE_NAMESPACE_RESOURCE_PREFIX)
                    .withDiscriminator(USER_CODE_NAMESPACE_DISCRIMINATOR_NAME, name)
                    .withTag(USER_CODE_NAMESPACE_RESOURCE_ID_TAG, id), resource));
        });
    }

    private record Namespace(MapResourceClassLoader classLoader, LocalUserCodeNamespaceStatsImpl stats) {
    }

    /** The class loader of a namespace; it applies the class filter to the classes the namespace defines. */
    private static final class NamespaceClassLoader extends MapResourceClassLoader {

        private final Map<String, byte[]> content;
        private final SerializationClassNameFilter classFilter;

        static {
            ClassLoader.registerAsParallelCapable();
        }

        NamespaceClassLoader(String namespace, ClassLoader parent, Map<String, byte[]> content,
                             SerializationClassNameFilter classFilter) {
            super(namespace, parent, () -> content, true);
            this.content = content;
            this.classFilter = classFilter;
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            if (classFilter != null && name != null && content.containsKey(classKeyName(toClassResourceId(name)))) {
                classFilter.filter(name);
            }
            return super.findClass(name);
        }
    }
}
