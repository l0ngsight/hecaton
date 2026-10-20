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

import uk.org.hecaton.cardinality.CardinalityEstimator;
import uk.org.hecaton.client.ClientService;
import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.cluster.Endpoint;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.DistributedObjectListener;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.ICacheManager;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.core.LifecycleService;
import uk.org.hecaton.cp.CPSubsystem;
import uk.org.hecaton.crdt.pncounter.PNCounter;
import uk.org.hecaton.dataconnection.DataConnectionService;
import uk.org.hecaton.durableexecutor.DurableExecutorService;
import uk.org.hecaton.flakeidgen.FlakeIdGenerator;
import uk.org.hecaton.instance.impl.executejar.ExecuteJobParameters;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.multimap.MultiMap;
import uk.org.hecaton.partition.PartitionService;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.scheduledexecutor.IScheduledExecutorService;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionService;
import uk.org.hecaton.sql.SqlService;
import uk.org.hecaton.topic.ITopic;
import uk.org.hecaton.transaction.HecatonXAResource;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionException;
import uk.org.hecaton.transaction.TransactionOptions;
import uk.org.hecaton.transaction.TransactionalTask;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

// A special HecatonInstance that has a BootstrappedJetProxy
@SuppressWarnings("checkstyle:methodcount")
public final class BootstrappedInstanceProxy implements HecatonInstance {

    private static final ILogger LOGGER = Logger.getLogger(BootstrappedInstanceProxy.class);

    private final HecatonInstance instance;

    private final BootstrappedJetProxy jetProxy;

    private boolean shutDownAllowed = true;

    BootstrappedInstanceProxy(HecatonInstance instance, BootstrappedJetProxy jetProxy) {
        this.instance = instance;
        this.jetProxy = jetProxy;
    }

    public List<Job> getSubmittedJobs() {
        ExecuteJobParameters executeJobParameters = jetProxy.getExecuteJobParameters();
        return executeJobParameters.getSubmittedJobs();
    }

    public BootstrappedInstanceProxy setShutDownAllowed(boolean shutDownAllowed) {
        this.shutDownAllowed = shutDownAllowed;
        return this;
    }

    public void setExecuteJobParameters(ExecuteJobParameters executeJobParameters) {
        jetProxy.setExecuteJobParameters(executeJobParameters);
    }

    public void removeExecuteJobParameters() {
        jetProxy.removeExecuteJobParameters();
    }

    @Nonnull
    @Override
    public String getName() {
        return instance.getName();
    }

    @Nonnull
    @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        return instance.getMap(name);
    }

    @Nonnull
    @Override
    public <E> IQueue<E> getQueue(@Nonnull String name) {
        return instance.getQueue(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getTopic(@Nonnull String name) {
        return instance.getTopic(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getReliableTopic(@Nonnull String name) {
        return instance.getReliableTopic(name);
    }

    @Nonnull
    @Override
    public <E> ISet<E> getSet(@Nonnull String name) {
        return instance.getSet(name);
    }

    @Nonnull
    @Override
    public <E> IList<E> getList(@Nonnull String name) {
        return instance.getList(name);
    }

    @Nonnull
    @Override
    public <K, V> MultiMap<K, V> getMultiMap(@Nonnull String name) {
        return instance.getMultiMap(name);
    }

    @Nonnull
    @Override
    public <E> Ringbuffer<E> getRingbuffer(@Nonnull String name) {
        return instance.getRingbuffer(name);
    }

    @Nonnull
    @Override
    public IExecutorService getExecutorService(@Nonnull String name) {
        return instance.getExecutorService(name);
    }

    @Nonnull
    @Override
    public DurableExecutorService getDurableExecutorService(@Nonnull String name) {
        return instance.getDurableExecutorService(name);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionalTask<T> task) throws TransactionException {
        return instance.executeTransaction(task);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionOptions options,
                                    @Nonnull TransactionalTask<T> task) throws TransactionException {
        return instance.executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return instance.newTransactionContext();
    }

    @Override
    public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
        return instance.newTransactionContext(options);
    }

    @Nonnull
    @Override
    public FlakeIdGenerator getFlakeIdGenerator(@Nonnull String name) {
        return instance.getFlakeIdGenerator(name);
    }

    @Nonnull
    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return instance.getReplicatedMap(name);
    }

    @Override
    public ICacheManager getCacheManager() {
        return instance.getCacheManager();
    }

    @Nonnull
    @Override
    public Cluster getCluster() {
        return instance.getCluster();
    }

    @Nonnull
    @Override
    public Endpoint getLocalEndpoint() {
        return instance.getLocalEndpoint();
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        return instance.getDistributedObjects();
    }

    @Nonnull
    @Override
    public Config getConfig() {
        return instance.getConfig();
    }

    @Nonnull
    @Override
    public PartitionService getPartitionService() {
        return instance.getPartitionService();
    }

    @Nonnull
    @Override
    public SplitBrainProtectionService getSplitBrainProtectionService() {
        return instance.getSplitBrainProtectionService();
    }

    @Nonnull
    @Override
    public ClientService getClientService() {
        return instance.getClientService();
    }

    @Nonnull
    @Override
    public LoggingService getLoggingService() {
        return instance.getLoggingService();
    }

    @Nonnull
    @Override
    public LifecycleService getLifecycleService() {
        return instance.getLifecycleService();
    }

    @Nonnull
    @Override
    public <T extends DistributedObject> T getDistributedObject(@Nonnull String serviceName, @Nonnull String name) {
        return instance.getDistributedObject(serviceName, name);
    }

    @Override
    public UUID addDistributedObjectListener(@Nonnull DistributedObjectListener distributedObjectListener) {
        return instance.addDistributedObjectListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(@Nonnull UUID registrationId) {
        return instance.removeDistributedObjectListener(registrationId);
    }

    @Nonnull
    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return instance.getUserContext();
    }

    @Nonnull
    @Override
    public HecatonXAResource getXAResource() {
        return instance.getXAResource();
    }

    @Nonnull
    @Override
    public CardinalityEstimator getCardinalityEstimator(@Nonnull String name) {
        return instance.getCardinalityEstimator(name);
    }

    @Nonnull
    @Override
    public PNCounter getPNCounter(@Nonnull String name) {
        return instance.getPNCounter(name);
    }

    @Nonnull
    @Override
    public IScheduledExecutorService getScheduledExecutorService(@Nonnull String name) {
        return instance.getScheduledExecutorService(name);
    }

    @Nonnull
    @Override
    public CPSubsystem getCPSubsystem() {
        return instance.getCPSubsystem();
    }

    @Nonnull
    @Override
    public SqlService getSql() {
        return instance.getSql();
    }

    @Nonnull
    @Override
    public DataConnectionService getDataConnectionService() {
        return instance.getDataConnectionService();
    }

    @Nonnull
    @Override
    public BootstrappedJetProxy getJet() {
        return jetProxy;
    }

    @Override
    public void shutdown() {
        if (shutDownAllowed) {
            getLifecycleService().shutdown();
        } else {
            LOGGER.severe("Shutdown of BootstrappedInstanceProxy is not allowed");
        }
    }
}
