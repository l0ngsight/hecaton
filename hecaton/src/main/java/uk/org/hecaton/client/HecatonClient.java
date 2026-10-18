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

package uk.org.hecaton.client;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientFailoverConfig;
import uk.org.hecaton.client.config.ClientNetworkConfig;
import uk.org.hecaton.client.impl.clientside.ClientConnectionManagerFactory;
import uk.org.hecaton.client.impl.clientside.DefaultClientConnectionManagerFactory;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.connection.AddressProvider;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.OutOfMemoryHandler;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory.InstanceFuture;
import uk.org.hecaton.instance.impl.OutOfMemoryErrorDispatcher;
import uk.org.hecaton.internal.util.EmptyStatement;
import uk.org.hecaton.internal.util.ExceptionUtil;
import uk.org.hecaton.internal.util.StringUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.client.impl.clientside.FailoverClientConfigSupport.resolveClientConfig;
import static uk.org.hecaton.client.impl.clientside.FailoverClientConfigSupport.resolveClientFailoverConfig;
import static uk.org.hecaton.internal.util.Preconditions.checkHasText;
import static uk.org.hecaton.internal.util.SetUtil.createHashSet;

/**
 * The HecatonClient is comparable to the {@link uk.org.hecaton.core.Hecaton} class and provides the ability
 * the create and manage Hecaton clients. Hecaton clients are {@link HecatonInstance} implementations, so
 * in most cases most of the code is unaware of talking to a cluster member or a client.
 * <p>
 * <h1>ALL_MEMBERS vs MULTI_MEMBER vs SINGLE_MEMBER routing clients</h1>
 * Hecaton Client enables you to do all Hecaton operations without being a member of the cluster. Clients can
 * use one of the following routing modes:
 * <ol>
 * <li>ALL_MEMBERS: client connects to all members of the cluster and this means that they immediately can send an
 * operation like map.get(key) to the member that owns that specific key.
 * </li>
 * <li>MULTI_MEMBER: client connects to a subset of members of the cluster, based on a provided grouping strategy.
 * This means that they can send an operation like map.get(key) to the member that owns that specific key, if
 * that member is available in this client's subset.
 * </li>
 * <li>SINGLE_MEMBER: it will connect to a random single member of the cluster and send requests to this member.
 * This member then needs to send the request to the correct member.
 * </li>
 * </ol>
 * For more information see {@link ClientNetworkConfig#getClusterRoutingConfig()}}.
 * <p>
 * <h1>High availability</h1>
 * When the connected cluster member dies, client will automatically switch to another live member.
 */
public final class HecatonClient {

    private static final AtomicInteger CLIENT_ID_GEN = new AtomicInteger();
    private static final ConcurrentMap<String, InstanceFuture<HecatonClientProxy>> CLIENTS = new ConcurrentHashMap<>(5);

    static {
        OutOfMemoryErrorDispatcher.setClientHandler(new ClientOutOfMemoryHandler());
    }

    private HecatonClient() {
    }

    /**
     * Creates a new HecatonInstance (a new client in a cluster).
     * This method allows you to create and run multiple instances
     * of Hecaton clients on the same JVM.
     * <p>
     * To shutdown all running HecatonInstances (all clients on this JVM)
     * call {@link #shutdownAll()}.
     * <p>
     * Hecaton will look into two places for the configuration file:
     * <ol>
     *     <li>
     *         System property: Hecaton will first check if "hecaton.client.config" system property is set to a file or a
     *         {@code classpath:...} path. The configuration can either be an XML or a YAML configuration, distinguished by the
     *         suffix ('.xml' or '.yaml') of the provided configuration file's name
     *         Examples: -Dhecaton.client.config=C:/myhecatonclient.xml ,
     *         -Dhecaton.client.config=classpath:the-hecaton-config.yaml ,
     *         -Dhecaton.client.config=classpath:com/mydomain/hecaton.xml
     *     </li>
     *     <li>
     *         "hecaton-client.xml" file in current working directory
     *     </li>
     *     <li>
     *         Classpath: Hecaton will check classpath for hecaton-client.xml file.
     *     </li>
     *     <li>
     *         "hecaton-client.yaml" file in current working directory
     *     </li>
     *     <li>
     *         Classpath: Hecaton will check classpath for hecaton-client.yaml file.
     *     </li>
     * </ol>
     * If Hecaton doesn't find any config file, it will start with the default configuration (hecaton-client-default.xml)
     * located in hecaton.jar.
     *
     * @return the new HecatonInstance
     * @see #shutdownAll()
     * @see #getHecatonClientByName(String) (String)
     */
    public static HecatonInstance newHecatonClient() {
        return newHecatonClientInternal(resolveClientConfig(null), null, null, null);
    }

    /**
     * Creates a new HecatonInstance (a new client in a cluster).
     * This method allows you to create and run multiple instances
     * of Hecaton clients on the same JVM.
     * <p>
     * To shutdown all running HecatonInstances (all clients on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @param config Configuration for the new HecatonInstance (member)
     * @return the new HecatonInstance
     * @see #shutdownAll()
     * @see #getHecatonClientByName(String) (String)
     */
    public static HecatonInstance newHecatonClient(ClientConfig config) {
        return newHecatonClientInternal(resolveClientConfig(config), null, null, null);
    }

    /**
     * Creates a client with cluster switch capability. Client will try to connect to alternative clusters according to
     * {@link ClientFailoverConfig} when it disconnects from a cluster.
     * <p>
     * The configuration is loaded using using the following resolution mechanism:
     * <ol>
     * <li>first it checks if a system property 'hecaton.client.failover.config' is set. If it exist and it begins with
     * 'classpath:', then a classpath resource is loaded. Else it will assume it is a file reference. The configuration can
     * either be an XML or a YAML configuration, distinguished by the suffix ('.xml' or '.yaml') of the provided configuration
     * file's name</li>
     * <li>it checks if a hecaton-client-failover.xml is available in the working dir</li>
     * <li>it checks if a hecaton-client-failover.xml is available on the classpath</li>
     * <li>it checks if a hecaton-client-failover.yaml is available in the working dir</li>
     * <li>it checks if a hecaton-client-failover.yaml is available on the classpath</li>
     * <li>if none available {@link HecatonException} is thrown</li>
     * </ol>
     *
     * @return the client instance
     * @throws HecatonException            if no failover configuration is found
     * @throws InvalidConfigurationException if the loaded failover configuration is not valid
     */
    public static HecatonInstance newHecatonFailoverClient() {
        return newHecatonClientInternal(null, resolveClientFailoverConfig(), null, null);
    }

    /**
     * Creates a client with cluster switch capability. Client will try to connect to alternative clusters according to
     * {@link ClientFailoverConfig} when it disconnects from a cluster.
     * <p>
     * If provided clientFailoverConfig is {@code null}, the configuration is loaded using using the following resolution
     * mechanism:
     * <ol>
     *      <li>first it checks if a system property 'hecaton.client.failover.config' is set. If it exist and it begins with
     *          'classpath:', then a classpath resource is loaded. Else it will assume it is a file reference. The configuration
     *          can either be an XML or a YAML configuration, distinguished by the suffix ('.xml' or '.yaml') of the provided
     *          configuration file's name</li>
     *      <li>it checks if a hecaton-client-failover.xml is available in the working dir</li>
     *      <li>it checks if a hecaton-client-failover.xml is available on the classpath</li>
     *      <li>it checks if a hecaton-client-failover.yaml is available in the working dir</li>
     *      <li>it checks if a hecaton-client-failover.yaml is available on the classpath</li>
     *      <li>if none available {@link HecatonException} is thrown</li>
     * </ol>
     *
     * @param clientFailoverConfig config describing the failover configs and try count
     * @return the client instance
     * @throws HecatonException            if no failover configuration is found
     * @throws InvalidConfigurationException if the provided or the loaded failover configuration is not valid
     */
    public static HecatonInstance newHecatonFailoverClient(ClientFailoverConfig clientFailoverConfig) {
        return newHecatonClientInternal(null, resolveClientFailoverConfig(clientFailoverConfig), null, null);
    }

    /**
     * Returns an existing HecatonClient with instanceName.
     *
     * @param instanceName Name of the HecatonInstance (client) which can be retrieved by {@link HecatonInstance#getName()}
     * @return HecatonInstance
     */
    public static HecatonInstance getHecatonClientByName(String instanceName) {
        InstanceFuture<HecatonClientProxy> future = CLIENTS.get(instanceName);
        if (future == null) {
            return null;
        }

        try {
            return future.get();
        } catch (IllegalStateException t) {
            return null;
        }
    }

    /**
     * Gets or creates a new HecatonInstance (a new client in a cluster) with the default XML configuration looked up in:
     * <ol>
     *     <li>
     *         System property: Hecaton will first check if "hecaton.client.config" system property is set to a file or a
     *         {@code classpath:...} path. The configuration can either be an XML or a YAML configuration, distinguished by the
     *         suffix ('.xml' or '.yaml') of the provided configuration file's name
     *         Examples: -Dhecaton.client.config=C:/myhecatonclient.xml ,
     *         -Dhecaton.client.config=classpath:the-hecaton-config.yaml ,
     *         -Dhecaton.client.config=classpath:com/mydomain/hecaton.xml
     *     </li>
     *     <li>
     *         "hecaton-client.xml" file in current working directory
     *     </li>
     *     <li>
     *         Classpath: Hecaton will check classpath for hecaton-client.xml file.
     *     </li>
     *     <li>
     *         "hecaton-client.yaml" file in current working directory
     *     </li>
     *     <li>
     *         Classpath: Hecaton will check classpath for hecaton-client.yaml file.
     *     </li>
     * </ol>
     * <p>
     * If a configuration file is not located, an {@link IllegalArgumentException} will be thrown.
     *
     * If a Hecaton instance with the same name as the configuration exists, then it is returned, otherwise it is created.
     *
     * @return the new HecatonInstance
     * @throws IllegalArgumentException if the instance name of the config is null or empty or if no config file can be
     *                                  located.
     * @see #getHecatonClientByName(String) (String)
     */
    public static HecatonInstance getOrCreateHecatonClient() {
        return getOrCreateClientInternal(null);
    }

    /**
     * Gets or creates a new HecatonInstance (a new client in a cluster) with a certain name.
     * <p>
     * If a Hecaton instance with the same name as the configuration exists, then it is returned, otherwise it is created.
     * <p>
     * If {@code config} is {@code null}, Hecaton will look into two places for the configuration file:
     * <ol>
     *     <li>
     *         System property: Hecaton will first check if "hecaton.client.config" system property is set to a file or a
     *         {@code classpath:...} path. The configuration can either be an XML or a YAML configuration, distinguished by the
     *         suffix ('.xml' or '.yaml') of the provided configuration file's name
     *         Examples: -Dhecaton.client.config=C:/myhecatonclient.xml ,
     *         -Dhecaton.client.config=classpath:the-hecaton-config.yaml ,
     *         -Dhecaton.client.config=classpath:com/mydomain/hecaton.xml
     *     </li>
     *     <li>
     *         "hecaton-client.xml" file in current working directory
     *     </li>
     *     <li>
     *         Classpath: Hecaton will check classpath for hecaton-client.xml file.
     *     </li>
     *     <li>
     *         "hecaton-client.yaml" file in current working directory
     *     </li>
     *     <li>
     *         Classpath: Hecaton will check classpath for hecaton-client.yaml file.
     *     </li>
     * </ol>
     * <p>
     * If a configuration file is not located, an {@link IllegalArgumentException} will be thrown.
     *
     * @param config Client configuration
     * @return the new HecatonInstance
     * @throws IllegalArgumentException if the instance name of the config is null or empty or if no config file can be
     *                                  located.
     * @see #getHecatonClientByName(String) (String)
     */
    public static HecatonInstance getOrCreateHecatonClient(ClientConfig config) {
        return getOrCreateClientInternal(config);
    }

    /**
     * Gets an immutable collection of all client HecatonInstances created in this JVM.
     * <p>
     * In managed environments such as Java EE or OSGi Hecaton can be loaded by multiple classloaders. Typically you will get
     * at least one classloader per every application deployed. In these cases only the client HecatonInstances created
     * by the same application will be seen, and instances created by different applications are invisible.
     * <p>
     * The returned collection is a snapshot of the client HecatonInstances. So changes to the client HecatonInstances
     * will not be visible in this collection.
     *
     * @return the collection of client HecatonInstances
     */
    public static Collection<HecatonInstance> getAllHecatonClients() {
        Set<HecatonInstance> result = createHashSet(CLIENTS.size());
        for (InstanceFuture<HecatonClientProxy> f : CLIENTS.values()) {
            result.add(f.get());
        }
        return Collections.unmodifiableCollection(result);
    }

    /**
     * Shuts down all the client HecatonInstance created in this JVM.
     * <p>
     * To be more precise it shuts down the HecatonInstances loaded using the same classloader this HecatonClient has been
     * loaded with.
     * <p>
     * This method is mostly used for testing purposes.
     *
     * @see #getAllHecatonClients()
     */
    public static void shutdownAll() {
        for (InstanceFuture<HecatonClientProxy> future : CLIENTS.values()) {
            try {
                HecatonClientProxy proxy = future.get();
                HecatonClientInstanceImpl client = proxy.client;
                if (client == null) {
                    continue;
                }
                proxy.client = null;
                client.shutdown();
            } catch (Throwable ignored) {
                EmptyStatement.ignore(ignored);
            }
        }
        OutOfMemoryErrorDispatcher.clearClients();
        CLIENTS.clear();
    }

    /**
     * Shutdown the provided client and remove it from the managed list
     *
     * @param instance the hecaton client instance
     */
    public static void shutdown(HecatonInstance instance) {
        if (instance instanceof HecatonClientProxy proxy) {
            HecatonClientInstanceImpl client = proxy.client;
            if (client == null) {
                return;
            }
            proxy.client = null;
            CLIENTS.remove(client.getName());

            try {
                client.shutdown();
            } catch (Throwable ignored) {
                EmptyStatement.ignore(ignored);
            } finally {
                OutOfMemoryErrorDispatcher.deregisterClient(client);
            }
        }
    }

    /**
     * Shutdown the provided client and remove it from the managed list
     *
     * @param instanceName the hecaton client instance name
     */
    public static void shutdown(String instanceName) {
        InstanceFuture<HecatonClientProxy> future = CLIENTS.remove(instanceName);
        if (future == null || !future.isSet()) {
            return;
        }

        HecatonClientProxy proxy = future.get();
        HecatonClientInstanceImpl client = proxy.client;
        if (client == null) {
            return;
        }
        proxy.client = null;
        try {
            client.shutdown();
        } catch (Throwable ignored) {
            EmptyStatement.ignore(ignored);
        } finally {
            OutOfMemoryErrorDispatcher.deregisterClient(client);
        }
    }

    /**
     * Sets <tt>OutOfMemoryHandler</tt> to be used when an <tt>OutOfMemoryError</tt>
     * is caught by Hecaton Client threads.
     * <p>
     * <b>Warning: </b> <tt>OutOfMemoryHandler</tt> may not be called although JVM throws
     * <tt>OutOfMemoryError</tt>.
     * Because error may be thrown from an external (user thread) thread
     * and Hecaton may not be informed about <tt>OutOfMemoryError</tt>.
     *
     * @param outOfMemoryHandler set when an <tt>OutOfMemoryError</tt> is caught by HecatonClient threads
     * @see OutOfMemoryError
     * @see OutOfMemoryHandler
     */
    public static void setOutOfMemoryHandler(OutOfMemoryHandler outOfMemoryHandler) {
        OutOfMemoryErrorDispatcher.setClientHandler(outOfMemoryHandler);
    }

    static HecatonInstance newHecatonClientInternal(
            ClientConfig config,
            ClientFailoverConfig failoverConfig,
            ClientConnectionManagerFactory connectionManagerFactory,
            AddressProvider addressProvider
    ) {
        checkConfigs(config, failoverConfig);
        String instanceName = getInstanceName(config, failoverConfig);

        InstanceFuture<HecatonClientProxy> future = new InstanceFuture<>();
        if (CLIENTS.putIfAbsent(instanceName, future) != null) {
            throw new InvalidConfigurationException("HecatonClientInstance with name '" + instanceName + "' already exists!");
        }

        try {
            return constructHecatonClient(instanceName, config, failoverConfig,
                    connectionManagerFactory, addressProvider, future);
        } catch (Throwable t) {
            CLIENTS.remove(instanceName, future);
            future.setFailure(t);
            throw ExceptionUtil.rethrow(t);
        }
    }

    private static HecatonInstance getOrCreateClientInternal(ClientConfig config) {
        config = resolveClientConfig(config);

        String instanceName = config.getInstanceName();
        checkHasText(instanceName, "instanceName must contain text");

        InstanceFuture<HecatonClientProxy> future = CLIENTS.get(instanceName);
        if (future != null) {
            return future.get();
        }

        future = new InstanceFuture<>();
        InstanceFuture<HecatonClientProxy> found = CLIENTS.putIfAbsent(instanceName, future);
        if (found != null) {
            return found.get();
        }

        try {
            return constructHecatonClient(instanceName, config, null, null, null, future);
        } catch (Throwable t) {
            CLIENTS.remove(instanceName, future);
            future.setFailure(t);
            throw ExceptionUtil.rethrow(t);
        }
    }

    private static HecatonInstance constructHecatonClient(
            String instanceName,
            ClientConfig config,
            ClientFailoverConfig failoverConfig,
            ClientConnectionManagerFactory connectionManagerFactory,
            AddressProvider addressProvider,
            InstanceFuture<HecatonClientProxy> future
    ) {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        HecatonClientProxy proxy;
        try {
            Thread.currentThread().setContextClassLoader(HecatonClient.class.getClassLoader());
            if (connectionManagerFactory == null) {
                connectionManagerFactory = new DefaultClientConnectionManagerFactory();
            }
            HecatonClientInstanceImpl client = new HecatonClientInstanceImpl(instanceName, config,
                    failoverConfig, connectionManagerFactory, addressProvider);
            client.start();
            OutOfMemoryErrorDispatcher.registerClient(client);
            proxy = new HecatonClientProxy(client);
            future.set(proxy);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
        return proxy;
    }

    private static void checkConfigs(ClientConfig config, ClientFailoverConfig failoverConfig) {
        assert config != null || failoverConfig != null : "At most one type of config can be provided";
        assert config == null || failoverConfig == null : "At least one config should be provided ";
    }

    private static String getInstanceName(ClientConfig config, ClientFailoverConfig failoverConfig) {
        int instanceNum = CLIENT_ID_GEN.incrementAndGet();
        String instanceName;
        if (config != null) {
            instanceName = config.getInstanceName();
        } else {
            instanceName = failoverConfig.getClientConfigs().get(0).getInstanceName();
        }
        if (StringUtil.isNullOrEmptyAfterTrim(instanceName)) {
            instanceName = "hc.client_" + instanceNum;
        }
        return instanceName;
    }
}
