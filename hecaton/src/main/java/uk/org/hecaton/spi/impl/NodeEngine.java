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

package uk.org.hecaton.spi.impl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.dataconnection.impl.InternalDataConnectionService;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeExtension;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.management.ManagementCenterService;
import uk.org.hecaton.internal.metrics.MetricsRegistry;
import uk.org.hecaton.internal.namespace.UserCodeNamespaceService;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.compact.schema.MemberSchemaService;
import uk.org.hecaton.internal.services.ManagedService;
import uk.org.hecaton.internal.tpc.TpcServerBootstrap;
import uk.org.hecaton.internal.util.phonehome.PhoneHome;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.spi.annotation.PrivateApi;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.spi.impl.executionservice.ExecutionService;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.impl.proxyservice.ProxyService;
import uk.org.hecaton.spi.impl.tenantcontrol.impl.TenantControlServiceImpl;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicyProvider;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionService;
import uk.org.hecaton.sql.impl.InternalSqlService;
import uk.org.hecaton.transaction.TransactionManagerService;
import uk.org.hecaton.version.MemberVersion;
import uk.org.hecaton.wan.impl.WanReplicationService;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.UUID;

/**
 * The NodeEngine is the 'umbrella' of services/service-method that gets injected into a {@link ManagedService}.
 * <p>
 * So if you are writing a custom SPI service, such as a stack-service, this service should probably implement
 * the {@link ManagedService} so you can get access to the services within the system.
 */
@PrivateApi
@SuppressWarnings("checkstyle:methodcount")
public interface NodeEngine {

    OperationService getOperationService();

    ExecutionService getExecutionService();

    ClusterService getClusterService();

    ManagementCenterService getManagementCenterService();

    IPartitionService getPartitionService();

    EventService getEventService();

    SerializationService getSerializationService();

    /**
     * Gets the compatibility serialization service for (de)serializing objects in a
     * format conforming with 3.x.
     */
    SerializationService getCompatibilitySerializationService();

    ProxyService getProxyService();

    /**
     * Returns the tenant control service.
     */
    TenantControlServiceImpl getTenantControlService();

    WanReplicationService getWanReplicationService();

    SplitBrainProtectionService getSplitBrainProtectionService();

    InternalSqlService getSqlService();

    /**
     * Return a service for accessing data connections
     */
    InternalDataConnectionService getDataConnectionService();

    /**
     * Gets the TransactionManagerService.
     *
     * @return the TransactionManagerService
     */
    TransactionManagerService getTransactionManagerService();

    /**
     * Gets the NamespaceService.
     *
     * @return the NamespaceService
     */
    UserCodeNamespaceService getNamespaceService();

    /**
     * Gets the address of the master member.
     * <p>
     * This value can be null if no master is elected yet.
     * <p>
     * The value can change over time.
     *
     * @return the address of the master member
     */
    Address getMasterAddress();

    /**
     * Get the address of this member.
     * <p>
     * The returned value will never change and will never be {@code null}.
     *
     * @return the address of this member
     */
    Address getThisAddress();

    /**
     * Returns the local member.
     * <p>
     * The returned value will never be null, but it may change when local lite member is promoted to a data member
     * or when a data member is demoted to a lite member or when this member merges to a new cluster after
     * split-brain detected. Returned value should not be cached but instead this method should be called each time
     * when local member is needed.
     *
     * @return the local member
     */
    Member getLocalMember();

    /**
     * Returns the Config that was used to create the HecatonInstance.
     * <p>
     * The returned value will never change and will never be {@code null}.
     *
     * @return the config
     */
    Config getConfig();

    /**
     * Returns the Config ClassLoader. This class loader will be used for the instantiation of all classes defined
     * by the configuration (e.g. listeners, policies, stores, partitioning strategies, split brain protection
     * functions, etc.).
     * <p>
     * When the {@link UserCodeNamespaceService} is enabled, this will return an instance of the
     * {@link uk.org.hecaton.internal.namespace.impl.NamespaceAwareClassLoader}, which delegates to child loaders
     * depending on the Namespace context of the class loading.
     * <p>
     * When the {@link UserCodeNamespaceService} is disabled, this will return either the legacy User Code Deployment
     * class loader if enabled, or else the {@link Config#getClassLoader()} will be returned.
     *
     * @return the config ClassLoader as defined above.
     */
    ClassLoader getConfigClassLoader();

    /**
     * Returns the HecatonProperties.
     * <p>
     * The returned value will never change and will never be {@code null}.
     *
     * @return the HecatonProperties
     */
    HecatonProperties getProperties();

    /**
     * Gets the logger for a given name.
     * <p>
     * It is best to get an ILogger through this method instead of calling {@link uk.org.hecaton.logging.Logger#getLogger(String)}.
     *
     * @param name the name of the logger
     * @return the ILogger
     * @throws NullPointerException if name is {@code null}
     * @see #getLogger(String)
     */
    ILogger getLogger(String name);

    /**
     * Gets the logger for a given class.
     * <p>
     * It is best to get an ILogger through this method instead of calling {@link uk.org.hecaton.logging.Logger#getLogger(Class)}.
     *
     * @param clazz the class of the logger
     * @return the ILogger
     * @throws NullPointerException if clazz is {@code null}
     * @see #getLogger(Class)
     */
    ILogger getLogger(Class clazz);

    /**
     * Serializes an object to a {@link Data}.
     * <p>
     * This method can safely be called with a {@link Data} instance. In that case, that instance is returned.
     * <p>
     * If this method is called with {@code null}, {@code null} is returned.
     *
     * @param object the object to serialize
     * @return the serialized object
     * @throws uk.org.hecaton.nio.serialization.HecatonSerializationException when serialization fails
     */
    Data toData(Object object);

    /**
     * Deserializes an object.
     * <p>
     * This method can safely be called on an object that is already deserialized. In that case, that instance
     * is returned.
     * <p>
     * If this method is called with {@code null}, {@code null} is returned.
     *
     * @param object the object to deserialize
     * @return the deserialized object
     * @throws uk.org.hecaton.nio.serialization.HecatonSerializationException when deserialization fails
     */
    <T> T toObject(Object object);

    /**
     * Deserializes an object.
     * <p>
     * This method can safely be called on an object that is already deserialized. In that case, that instance
     * is returned.
     * <p>
     * If this method is called with {@code null}, {@code null} is returned.
     *
     * @param object the object to deserialize
     * @param klazz  The class to instantiate when deserializing the object
     * @return the deserialized object
     * @throws uk.org.hecaton.nio.serialization.HecatonSerializationException when deserialization fails
     */
    <T> T toObject(Object object, Class klazz);

    /**
     * Indicates that node is not shutting down or it has not already shut down
     *
     * @return {@code true} if node is not shutting down or it has not already shut down, {@code false} otherwise
     */
    boolean isRunning();

    /**
     * @return      {@code true} if this {@code Node} has completed startup, {@code false} otherwise.
     * @see         NodeExtension#isStartCompleted()
     */
    boolean isStartCompleted();

    /**
     * Returns the HecatonInstance that this {@link NodeEngine} belongs to.
     *
     * @return the HecatonInstance
     */
    HecatonInstance getHecatonInstance();

    /**
     * Gets the service with the given name.
     *
     * @param serviceName the name of the service
     * @param <T>         the type of the service
     * @return the found service, or HecatonException in case of failure ({@code null} will never be returned)
     */
    <T> T getService(@Nonnull String serviceName);

    /**
     * Gets the service for the given serviceName if it exists or null otherwise.
     *
     * @param serviceName the name of the shared service to get
     * @param <T>         the type of the service
     * @return the found service, or null if the service was not found
     * @throws NullPointerException if the serviceName is {@code null}
     */
    <T> T getServiceOrNull(@Nonnull String serviceName);

    /**
     * Returns the codebase version of the node. For example, when running on hecaton-3.8.jar, this would resolve
     * to {@code Version.of(3,8,0)}. A node's codebase version may be different than cluster version.
     *
     * @return codebase version of the node
     * @see Cluster#getClusterVersion()
     * @since 3.8
     */
    MemberVersion getVersion();

    /**
     * Returns the {@link SplitBrainMergePolicyProvider} for this instance.
     *
     * @return the {@link SplitBrainMergePolicyProvider}
     * @since 3.10
     */
    SplitBrainMergePolicyProvider getSplitBrainMergePolicyProvider();

    /**
     * Returns a list of services matching provides service class/interface.
     * <p>
     * <b>Note:</b> CoreServices will be placed at the beginning of the list.
     */
    <S> Collection<S> getServices(Class<S> serviceClass);

    /**
     * Get member schema service.
     * <p>
     * @return the {@link MemberSchemaService}
     */
    MemberSchemaService getSchemaService();

    /**
     * Get current node.
     * <p>
     * @return the {@link Node}
     */
    Node getNode();

    /**
     * Get logging service.
     * <p>
     * @return the {@link LoggingService}
     */
    LoggingService getLoggingService();

    /**
     * Get metrics registry.
     * <p>
     * @return the {@link MetricsRegistry}
     */
    MetricsRegistry getMetricsRegistry();

    PhoneHome getPhoneHome();

    void onClientDisconnected(UUID clientUuid);

    TpcServerBootstrap getTpcServerBootstrap();
}
