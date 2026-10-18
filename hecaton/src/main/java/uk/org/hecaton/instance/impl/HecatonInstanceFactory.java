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

package uk.org.hecaton.instance.impl;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.jmx.ManagementService;
import uk.org.hecaton.internal.util.ExceptionUtil;
import uk.org.hecaton.internal.util.ModularJavaUtils;
import uk.org.hecaton.internal.util.StringUtil;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.spi.properties.ClusterProperty;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.STARTED;
import static uk.org.hecaton.instance.impl.DuplicatedResourcesScanner.checkForDuplicates;
import static uk.org.hecaton.internal.util.EmptyStatement.ignore;
import static uk.org.hecaton.internal.util.Preconditions.checkHasText;
import static uk.org.hecaton.internal.util.SetUtil.createHashSet;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Central manager for all Hecaton members of the JVM.
 * <p>
 * All creation functionality will be stored here and a particular instance of a member will delegate here.
 */
@SuppressWarnings("SynchronizationOnStaticField")
public final class HecatonInstanceFactory {

    private static final int ADDITIONAL_SLEEP_SECONDS_FOR_NON_FIRST_MEMBERS = 4;

    private static final AtomicInteger FACTORY_ID_GEN = new AtomicInteger();
    private static final ConcurrentMap<String, InstanceFuture<HecatonInstanceProxy>> INSTANCE_MAP = new ConcurrentHashMap<>(5);

    private static final ILogger LOGGER = Logger.getLogger(HecatonInstanceFactory.class);

    static {
        ModularJavaUtils.checkJavaInternalAccess(LOGGER);
        String resourceName = "META-INF/services/" + NodeExtension.class.getName();
        checkForDuplicates(HecatonInstanceFactory.class.getClassLoader(), LOGGER, resourceName);
    }

    private HecatonInstanceFactory() {
    }

    public static Set<HecatonInstance> getAllHecatonInstances() {
        Set<HecatonInstance> result = createHashSet(INSTANCE_MAP.size());
        for (InstanceFuture<HecatonInstanceProxy> f : INSTANCE_MAP.values()) {
            result.add(f.get());
        }
        return result;
    }

    public static HecatonInstance getHecatonInstance(String instanceName) {
        InstanceFuture<HecatonInstanceProxy> instanceFuture = INSTANCE_MAP.get(instanceName);
        if (instanceFuture == null) {
            return null;
        }

        try {
            return instanceFuture.get();
        } catch (IllegalStateException t) {
            return null;
        }
    }

    public static HecatonInstance getOrCreateHecatonInstance(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }

        String name = config.getInstanceName();
        checkHasText(name, "instanceName must contain text");

        InstanceFuture<HecatonInstanceProxy> future = INSTANCE_MAP.get(name);
        if (future != null) {
            return future.get();
        }

        future = new InstanceFuture<>();
        InstanceFuture<HecatonInstanceProxy> found = INSTANCE_MAP.putIfAbsent(name, future);
        if (found != null) {
            return found.get();
        }

        try {
            return constructHecatonInstance(config, name, new DefaultNodeContext(), future);
        } catch (Throwable t) {
            INSTANCE_MAP.remove(name, future);
            future.setFailure(t);
            throw ExceptionUtil.rethrow(t);
        }
    }

    /**
     * Creates a new Hecaton instance.
     *
     * @param config the configuration to use; if <code>null</code>, the set of defaults
     *               as specified in the XSD for the configuration XML will be used.
     * @return the configured {@link HecatonInstance}
     */
    public static HecatonInstance newHecatonInstance(Config config) {
        if (config == null) {
            config = Config.load();
        }

        return newHecatonInstance(
                config,
                config.getInstanceName(),
                new DefaultNodeContext()
        );
    }

    public static String createInstanceName(Config config) {
        String propertyValue = config.getProperty(ClusterProperty.MOBY_NAMING_ENABLED.getName());
        if (propertyValue == null) {
            propertyValue = ClusterProperty.MOBY_NAMING_ENABLED.getDefaultValue();
        }
        boolean useMobyNaming = Boolean.parseBoolean(propertyValue);
        int instanceNum = FACTORY_ID_GEN.incrementAndGet();
        String name;
        if (useMobyNaming) {
            name = MobyNames.getRandomName(instanceNum);
        } else {
            name = "_hzInstance_" + instanceNum + "_" + config.getClusterName();
        }
        return name;
    }

    /**
     * Return real name for the hecaton instance's instance
     *
     * @param instanceName -  template of the name
     * @param config       -   config
     * @return -   real hecaton instance's name
     */
    public static String getInstanceName(String instanceName, Config config) {
        String name = instanceName;

        if (StringUtil.isNullOrEmptyAfterTrim(name)) {
            name = createInstanceName(config);
        }

        return name;
    }

    /**
     * Creates a new Hecaton instance.
     *
     * @param config       the configuration to use; if <code>null</code>, the set of defaults
     *                     as specified in the XSD for the configuration XML will be used.
     * @param instanceName the name of the {@link HecatonInstance}
     * @param nodeContext  the {@link NodeContext} to use
     * @return the configured {@link HecatonInstance}
     */
    public static HecatonInstance newHecatonInstance(Config config, String instanceName, NodeContext nodeContext) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }

        String name = getInstanceName(instanceName, config);

        InstanceFuture<HecatonInstanceProxy> future = new InstanceFuture<>();
        if (INSTANCE_MAP.putIfAbsent(name, future) != null) {
            throw new InvalidConfigurationException("HecatonInstance with name '" + name + "' already exists!");
        }

        try {
            return constructHecatonInstance(config, name, nodeContext, future);
        } catch (Throwable t) {
            INSTANCE_MAP.remove(name, future);
            future.setFailure(t);
            throw ExceptionUtil.rethrow(t);
        }
    }

    private static HecatonInstanceProxy newHecatonProxy(HecatonInstanceImpl hecatonInstance) {
        return new HecatonInstanceProxy(hecatonInstance);
    }

    private static HecatonInstanceProxy constructHecatonInstance(Config config, String instanceName, NodeContext nodeContext,
                                                                     InstanceFuture<HecatonInstanceProxy> future) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        HecatonInstanceProxy proxy;
        try {
            if (classLoader == null) {
                Thread.currentThread().setContextClassLoader(HecatonInstanceFactory.class.getClassLoader());
            }
            HecatonInstanceImpl hecatonInstance = new HecatonInstanceImpl(instanceName, config, nodeContext);
            OutOfMemoryErrorDispatcher.registerServer(hecatonInstance);

            proxy = newHecatonProxy(hecatonInstance);

            Node node = hecatonInstance.node;
            boolean firstMember = isFirstMember(node);
            long initialWaitSeconds = node.getProperties().getSeconds(ClusterProperty.INITIAL_WAIT_SECONDS);
            if (initialWaitSeconds > 0) {
                hecatonInstance.logger.info(format("Waiting %d seconds before completing HecatonInstance startup...",
                        initialWaitSeconds));
                try {
                    SECONDS.sleep(initialWaitSeconds);
                    if (firstMember) {
                        node.partitionService.firstArrangement();
                    } else {
                        SECONDS.sleep(ADDITIONAL_SLEEP_SECONDS_FOR_NON_FIRST_MEMBERS);
                    }
                } catch (InterruptedException ignored) {
                    currentThread().interrupt();
                }
            }
            awaitMinimalClusterSize(hecatonInstance, node, firstMember);
            future.set(proxy);
            hecatonInstance.lifecycleService.fireLifecycleEvent(STARTED);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
        return proxy;
    }

    private static boolean isFirstMember(Node node) {
        Iterator<Member> iterator = node.getClusterService().getMembers().iterator();
        return (iterator.hasNext() && iterator.next().localMember());
    }

    private static void awaitMinimalClusterSize(HecatonInstanceImpl hecatonInstance, Node node, boolean firstMember)
            throws InterruptedException {

        int initialMinClusterSize = node.getProperties().getInteger(ClusterProperty.INITIAL_MIN_CLUSTER_SIZE);
        while (node.getClusterService().getSize() < initialMinClusterSize) {
            try {
                hecatonInstance.logger.info("HecatonInstance waiting for cluster size of " + initialMinClusterSize);
                SECONDS.sleep(1);
            } catch (InterruptedException ignored) {
                currentThread().interrupt();
            }
        }
        if (initialMinClusterSize > 1) {
            if (firstMember) {
                node.partitionService.firstArrangement();
            } else {
                SECONDS.sleep(3);
            }
            hecatonInstance.logger.info("HecatonInstance starting after waiting for cluster size of "
                    + initialMinClusterSize);
        }
    }

    public static void shutdownAll() {
        shutdownAll(false);
    }

    public static void terminateAll() {
        shutdownAll(true);
    }

    private static void shutdownAll(boolean terminate) {
        List<HecatonInstanceProxy> instances = new LinkedList<>();
        for (InstanceFuture<HecatonInstanceProxy> future : INSTANCE_MAP.values()) {
            try {
                HecatonInstanceProxy instanceProxy = future.get();
                instances.add(instanceProxy);
            } catch (RuntimeException ignored) {
                ignore(ignored);
            }
        }

        INSTANCE_MAP.clear();
        OutOfMemoryErrorDispatcher.clearServers();
        ManagementService.shutdownAll(instances);
        instances.sort(Comparator.comparing(HecatonInstanceProxy::getName));
        for (HecatonInstanceProxy proxy : instances) {
            if (terminate) {
                proxy.getLifecycleService().terminate();
            } else {
                proxy.getLifecycleService().shutdown();
            }
            proxy.original = null;
        }
    }

    public static void remove(HecatonInstanceImpl instance) {
        OutOfMemoryErrorDispatcher.deregisterServer(instance);
        InstanceFuture<HecatonInstanceProxy> future = INSTANCE_MAP.remove(instance.getName());
        if (future != null && future.isSet()) {
            future.get().original = null;
        }
        if (INSTANCE_MAP.isEmpty()) {
            ManagementService.shutdown(instance.getName());
        }
    }

    public static class InstanceFuture<T> {
        private volatile T hz;
        private volatile Throwable throwable;

        public T get() {
            if (hz != null) {
                return hz;
            }

            boolean restoreInterrupt = false;
            synchronized (this) {
                while (hz == null && throwable == null) {
                    try {
                        wait();
                    } catch (InterruptedException ignored) {
                        restoreInterrupt = true;
                    }
                }
            }

            if (restoreInterrupt) {
                Thread.currentThread().interrupt();
            }

            if (hz != null) {
                return hz;
            }

            throw new IllegalStateException(throwable);
        }

        public void set(T proxy) {
            synchronized (this) {
                this.hz = proxy;
                notifyAll();
            }
        }

        public void setFailure(Throwable throwable) {
            synchronized (this) {
                this.throwable = throwable;
                notifyAll();
            }
        }

        public boolean isSet() {
            return hz != null;
        }
    }

}
