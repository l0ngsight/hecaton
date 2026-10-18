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

import uk.org.hecaton.cache.impl.HecatonInstanceCacheManager;
import uk.org.hecaton.cardinality.CardinalityEstimator;
import uk.org.hecaton.cardinality.impl.CardinalityEstimatorService;
import uk.org.hecaton.client.ClientService;
import uk.org.hecaton.client.impl.ClientServiceProxy;
import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.collection.impl.list.ListService;
import uk.org.hecaton.collection.impl.queue.QueueService;
import uk.org.hecaton.collection.impl.set.SetService;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.DistributedObjectListener;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.core.ManagedContext;
import uk.org.hecaton.cp.CPSubsystem;
import uk.org.hecaton.crdt.pncounter.PNCounter;
import uk.org.hecaton.dataconnection.DataConnectionService;
import uk.org.hecaton.durableexecutor.DurableExecutorService;
import uk.org.hecaton.durableexecutor.impl.DistributedDurableExecutorService;
import uk.org.hecaton.executor.impl.DistributedExecutorService;
import uk.org.hecaton.flakeidgen.FlakeIdGenerator;
import uk.org.hecaton.flakeidgen.impl.FlakeIdGeneratorService;
import uk.org.hecaton.internal.crdt.pncounter.PNCounterService;
import uk.org.hecaton.internal.jmx.ManagementService;
import uk.org.hecaton.internal.memory.MemoryStats;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.jet.JetService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.multimap.MultiMap;
import uk.org.hecaton.multimap.impl.MultiMapService;
import uk.org.hecaton.partition.PartitionService;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.ringbuffer.impl.RingbufferService;
import uk.org.hecaton.scheduledexecutor.IScheduledExecutorService;
import uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService;
import uk.org.hecaton.spi.impl.SerializationServiceSupport;
import uk.org.hecaton.spi.impl.proxyservice.ProxyService;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionService;
import uk.org.hecaton.sql.SqlService;
import uk.org.hecaton.topic.ITopic;
import uk.org.hecaton.topic.impl.TopicService;
import uk.org.hecaton.topic.impl.reliable.ReliableTopicService;
import uk.org.hecaton.transaction.HecatonXAResource;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionException;
import uk.org.hecaton.transaction.TransactionManagerService;
import uk.org.hecaton.transaction.TransactionOptions;
import uk.org.hecaton.transaction.TransactionalTask;
import uk.org.hecaton.transaction.impl.xa.XAService;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.internal.util.EmptyStatement.ignore;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public class HecatonInstanceImpl implements HecatonInstance, SerializationServiceSupport {

    @SuppressWarnings("checkstyle:visibilitymodifier")
    public final Node node;

    final ConcurrentMap<String, Object> userContext = new ConcurrentHashMap<>();

    final ILogger logger;
    final String name;
    final ManagementService managementService;
    final LifecycleServiceImpl lifecycleService;
    final CPSubsystem cpSubsystem;
    final ManagedContext managedContext;
    final HecatonInstanceCacheManager hecatonCacheManager;

    @SuppressWarnings("checkstyle:executablestatementcount")
    protected HecatonInstanceImpl(String name, Config config, NodeContext nodeContext) {
        this.name = name;
        this.lifecycleService = new LifecycleServiceImpl(this);

        ManagedContext configuredManagedContext = config.getManagedContext();
        this.managedContext = new HecatonManagedContext(this, configuredManagedContext);

        // we are going to copy the user-context map of the Config so that each HecatonInstance will get its own
        // user-context map instance instead of having a shared map instance. So changes made to the user-context map
        // in one HecatonInstance will not reflect on other the user-context of other HecatonInstances
        this.userContext.putAll(config.getUserContext());
        this.node = createNode(config, nodeContext);
        this.cpSubsystem = this.node.getNodeExtension().createCPSubsystem(node.getNodeEngine());

        try {
            this.logger = node.getLogger(getClass().getName());

            node.start();
            if (!node.isRunning()) {
                throw new IllegalStateException("Node failed to start!");
            }

            this.managementService = node.getNodeExtension().createJMXManagementService(this);
            initManagedContext(configuredManagedContext);

            this.hecatonCacheManager = new HecatonInstanceCacheManager(this);
            ClassLoader classLoader = node.getConfigClassLoader();
            if (classLoader instanceof HecatonInstanceAware aware) {
                aware.setHecatonInstance(this);
            }
        } catch (Throwable e) {
            //noinspection CatchMayIgnoreException
            try {
                // terminate the node by terminating the NodeEngine, ConnectionManager, services, operation threads etc.
                node.shutdown(true);
            } catch (Throwable ignored) {
                ignore(ignored);
            }
            throw rethrow(e);
        }
    }

    private Node createNode(Config config, NodeContext nodeContext) {
        return new Node(this, config, nodeContext);
    }

    private void initManagedContext(ManagedContext configuredManagedContext) {
        if (configuredManagedContext != null) {
            if (configuredManagedContext instanceof HecatonInstanceAware aware) {
                aware.setHecatonInstance(this);
            }
        }
    }

    public ManagementService getManagementService() {
        return managementService;
    }

    /**
     * Indicates that instance is not shutting down, or it has not already shut down
     *
     * @return true if instance is not shutting down, or it has not already shut down
     */
    public boolean isRunning() {
        return node.isRunning();
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        checkNotNull(name, "Retrieving a map instance with a null name is not allowed!");
        return getDistributedObject(MapService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> IQueue<E> getQueue(@Nonnull String name) {
        checkNotNull(name, "Retrieving a queue instance with a null name is not allowed!");
        return getDistributedObject(QueueService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getTopic(@Nonnull String name) {
        checkNotNull(name, "Retrieving a topic instance with a null name is not allowed!");
        return getDistributedObject(TopicService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getReliableTopic(@Nonnull String name) {
        checkNotNull(name, "Retrieving a topic instance with a null name is not allowed!");
        return getDistributedObject(ReliableTopicService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> ISet<E> getSet(@Nonnull String name) {
        checkNotNull(name, "Retrieving a set instance with a null name is not allowed!");
        return getDistributedObject(SetService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> IList<E> getList(@Nonnull String name) {
        checkNotNull(name, "Retrieving a list instance with a null name is not allowed!");
        return getDistributedObject(ListService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <K, V> MultiMap<K, V> getMultiMap(@Nonnull String name) {
        checkNotNull(name, "Retrieving a multi-map instance with a null name is not allowed!");
        return getDistributedObject(MultiMapService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> Ringbuffer<E> getRingbuffer(@Nonnull String name) {
        checkNotNull(name, "Retrieving a ringbuffer instance with a null name is not allowed!");
        return getDistributedObject(RingbufferService.SERVICE_NAME, name);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionalTask<T> task)
            throws TransactionException {
        return executeTransaction(TransactionOptions.getDefault(), task);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionOptions options,
                                    @Nonnull TransactionalTask<T> task) throws TransactionException {
        TransactionManagerService transactionManagerService = node.getNodeEngine().getTransactionManagerService();
        return transactionManagerService.executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return newTransactionContext(TransactionOptions.getDefault());
    }

    @Override
    public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
        checkNotNull(options, "TransactionOptions must not be null!");
        TransactionManagerService transactionManagerService = node.getNodeEngine().getTransactionManagerService();
        return transactionManagerService.newTransactionContext(options);
    }

    @Nonnull
    @Override
    public IExecutorService getExecutorService(@Nonnull String name) {
        checkNotNull(name, "Retrieving an executor instance with a null name is not allowed!");
        return getDistributedObject(DistributedExecutorService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public DurableExecutorService getDurableExecutorService(@Nonnull String name) {
        checkNotNull(name, "Retrieving a durable executor instance with a null name is not allowed!");
        return getDistributedObject(DistributedDurableExecutorService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public FlakeIdGenerator getFlakeIdGenerator(@Nonnull String name) {
        checkNotNull(name, "Retrieving a Flake ID-generator instance with a null name is not allowed!");
        return getDistributedObject(FlakeIdGeneratorService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        checkNotNull(name, "Retrieving a replicated map instance with a null name is not allowed!");
        return getDistributedObject(ReplicatedMapService.SERVICE_NAME, name);
    }

    @Override
    public HecatonInstanceCacheManager getCacheManager() {
        return hecatonCacheManager;
    }

    @Nonnull
    @Override
    public Cluster getCluster() {
        return node.getClusterService();
    }

    @Nonnull
    @Override
    public Member getLocalEndpoint() {
        return node.getLocalMember();
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        ProxyService proxyService = node.getNodeEngine().getProxyService();
        return proxyService.getAllDistributedObjects();
    }

    @Nonnull
    @Override
    public Config getConfig() {
        return node.getConfig();
    }

    @Nonnull
    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return userContext;
    }

    @Nonnull
    @Override
    public PartitionService getPartitionService() {
        return node.getPartitionService().getPartitionServiceProxy();
    }

    @Nonnull
    @Override
    public SplitBrainProtectionService getSplitBrainProtectionService() {
        return node.getNodeEngine().getSplitBrainProtectionService();
    }

    @Nonnull
    @Override
    public ClientService getClientService() {
        return new ClientServiceProxy(node);
    }

    @Nonnull
    @Override
    public LoggingService getLoggingService() {
        return node.getLoggingService();
    }

    @Nonnull
    @Override
    public LifecycleServiceImpl getLifecycleService() {
        return lifecycleService;
    }

    @Override
    public void shutdown() {
        getLifecycleService().shutdown();
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <T extends DistributedObject> T getDistributedObject(@Nonnull String serviceName, @Nonnull String name) {
        ProxyService proxyService = node.getNodeEngine().getProxyService();
        UUID source = node.nodeEngine.getLocalMember().getUuid();
        return (T) proxyService.getDistributedObject(serviceName, name, source);
    }

    @Override
    public UUID addDistributedObjectListener(@Nonnull DistributedObjectListener distributedObjectListener) {
        checkNotNull(distributedObjectListener, "DistributedObjectListener must not be null!");
        final ProxyService proxyService = node.getNodeEngine().getProxyService();
        return proxyService.addProxyListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "Registration ID must not be null!");
        final ProxyService proxyService = node.getNodeEngine().getProxyService();
        return proxyService.removeProxyListener(registrationId);
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return node.getSerializationService();
    }

    public MemoryStats getMemoryStats() {
        return node.getNodeExtension().getMemoryStats();
    }

    @Nonnull
    @Override
    public HecatonXAResource getXAResource() {
        return getDistributedObject(XAService.SERVICE_NAME, XAService.SERVICE_NAME);
    }

    @Nonnull
    @Override
    public CardinalityEstimator getCardinalityEstimator(@Nonnull String name) {
        checkNotNull(name, "Retrieving a cardinality estimator instance with a null name is not allowed!");
        return getDistributedObject(CardinalityEstimatorService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public PNCounter getPNCounter(@Nonnull String name) {
        checkNotNull(name, "Retrieving a PN counter instance with a null name is not allowed!");
        return getDistributedObject(PNCounterService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public IScheduledExecutorService getScheduledExecutorService(@Nonnull String name) {
        checkNotNull(name, "Retrieving a scheduled executor instance with a null name is not allowed!");
        return getDistributedObject(DistributedScheduledExecutorService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public CPSubsystem getCPSubsystem() {
        return cpSubsystem;
    }

    @Nonnull
    @Override
    public SqlService getSql() {
        return node.getNodeEngine().getSqlService();
    }

    @Nonnull
    @Override
    public DataConnectionService getDataConnectionService() {
        return node.getNodeEngine().getDataConnectionService();
    }

    @Nonnull
    @Override
    public JetService getJet() {
        return node.getNodeExtension().getJet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HecatonInstance that)) {
            return false;
        }
        return Objects.equals(this.name, that.getName());
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "HecatonInstance{name='" + name + "', node=" + node.getThisAddress() + '}';
    }
}
