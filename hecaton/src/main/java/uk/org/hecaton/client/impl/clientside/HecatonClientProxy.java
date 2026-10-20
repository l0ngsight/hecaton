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

package uk.org.hecaton.client.impl.clientside;

import uk.org.hecaton.cardinality.CardinalityEstimator;
import uk.org.hecaton.client.Client;
import uk.org.hecaton.client.ClientService;
import uk.org.hecaton.client.HecatonClientNotActiveException;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.cluster.Cluster;
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
import uk.org.hecaton.instance.impl.TerminatedLifecycleService;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.jet.JetService;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.multimap.MultiMap;
import uk.org.hecaton.partition.PartitionService;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.scheduledexecutor.IScheduledExecutorService;
import uk.org.hecaton.spi.impl.SerializationServiceSupport;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * A client-side proxy {@link uk.org.hecaton.core.HecatonInstance} instance.
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity", "MethodCount", "VisibilityModifier"})
public class HecatonClientProxy implements HecatonInstance, SerializationServiceSupport {

    public volatile HecatonClientInstanceImpl client;

    public HecatonClientProxy(HecatonClientInstanceImpl client) {
        this.client = client;
    }

    @Nonnull
    @Override
    public Config getConfig() {
        return getClient().getConfig();
    }

    @Nonnull
    @Override
    public String getName() {
        return getClient().getName();
    }

    @Nonnull
    @Override
    public <E> Ringbuffer<E> getRingbuffer(@Nonnull String name) {
        return getClient().getRingbuffer(name);
    }

    @Nonnull
    @Override
    public <E> IQueue<E> getQueue(@Nonnull String name) {
        return getClient().getQueue(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getTopic(@Nonnull String name) {
        return getClient().getTopic(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getReliableTopic(@Nonnull String name) {
        return getClient().getReliableTopic(name);
    }

    @Nonnull
    @Override
    public <E> ISet<E> getSet(@Nonnull String name) {
        return getClient().getSet(name);
    }

    @Nonnull
    @Override
    public <E> IList<E> getList(@Nonnull String name) {
        return getClient().getList(name);
    }

    @Nonnull
    @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        return getClient().getMap(name);
    }

    @Nonnull
    @Override
    public <K, V> MultiMap<K, V> getMultiMap(@Nonnull String name) {
        return getClient().getMultiMap(name);
    }

    @Nonnull
    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return getClient().getReplicatedMap(name);
    }

    @Override
    public ICacheManager getCacheManager() {
        return getClient().getCacheManager();
    }

    @Nonnull
    @Override
    public Cluster getCluster() {
        return getClient().getCluster();
    }

    @Nonnull
    @Override
    public Client getLocalEndpoint() {
        return getClient().getLocalEndpoint();
    }

    @Nonnull
    @Override
    public IExecutorService getExecutorService(@Nonnull String name) {
        return getClient().getExecutorService(name);
    }

    @Nonnull
    @Override
    public DurableExecutorService getDurableExecutorService(@Nonnull String name) {
        return getClient().getDurableExecutorService(name);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionalTask<T> task)
            throws TransactionException {
        return getClient().executeTransaction(task);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionOptions options, @Nonnull TransactionalTask<T> task)
            throws TransactionException {
        return getClient().executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return getClient().newTransactionContext();
    }

    @Override
    public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
        return getClient().newTransactionContext(options);
    }

    @Nonnull
    @Override
    public FlakeIdGenerator getFlakeIdGenerator(@Nonnull String name) {
        return getClient().getFlakeIdGenerator(name);
    }

    @Nonnull
    @Override
    public CardinalityEstimator getCardinalityEstimator(@Nonnull String name) {
        return getClient().getCardinalityEstimator(name);
    }

    @Nonnull
    @Override
    public PNCounter getPNCounter(@Nonnull String name) {
        return getClient().getPNCounter(name);
    }

    @Nonnull
    @Override
    public IScheduledExecutorService getScheduledExecutorService(@Nonnull String name) {
        return getClient().getScheduledExecutorService(name);
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        return getClient().getDistributedObjects();
    }

    @Override
    public UUID addDistributedObjectListener(@Nonnull DistributedObjectListener distributedObjectListener) {
        return getClient().addDistributedObjectListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(@Nonnull UUID registrationId) {
        return getClient().removeDistributedObjectListener(registrationId);
    }

    @Nonnull
    @Override
    public PartitionService getPartitionService() {
        return getClient().getPartitionService();
    }

    @Nonnull
    @Override
    public SplitBrainProtectionService getSplitBrainProtectionService() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public ClientService getClientService() {
        return getClient().getClientService();
    }

    @Nonnull
    @Override
    public LoggingService getLoggingService() {
        return getClient().getLoggingService();
    }

    @Nonnull
    @Override
    public LifecycleService getLifecycleService() {
        final HecatonClientInstanceImpl hz = client;
        return hz != null ? hz.getLifecycleService() : new TerminatedLifecycleService();
    }

    @Nonnull
    @Override
    public <T extends DistributedObject> T getDistributedObject(@Nonnull String serviceName, @Nonnull String name) {
        return getClient().getDistributedObject(serviceName, name);
    }

    @Nonnull
    @Override
    public CPSubsystem getCPSubsystem() {
        return getClient().getCPSubsystem();
    }

    @Nonnull
    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return getClient().getUserContext();
    }

    public ClientConfig getClientConfig() {
        return getClient().getClientConfig();
    }

    @Nonnull
    @Override
    public HecatonXAResource getXAResource() {
        return getClient().getXAResource();
    }

    @Override
    public void shutdown() {
        getLifecycleService().shutdown();
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return getClient().getSerializationService();
    }

    protected HecatonClientInstanceImpl getClient() {
        final HecatonClientInstanceImpl c = client;
        if (c == null || !c.getLifecycleService().isRunning()) {
            throw new HecatonClientNotActiveException();
        }
        return c;
    }

    @Override
    public String toString() {
        final HecatonClientInstanceImpl hecatonInstance = client;
        if (hecatonInstance != null) {
            return hecatonInstance.toString();
        }
        return "HecatonClientInstance {NOT ACTIVE}";
    }

    @Nonnull
    @Override
    public SqlService getSql() {
        return getClient().getSql();
    }

    @Nonnull
    @Override
    public DataConnectionService getDataConnectionService() {
        return getClient().getDataConnectionService();
    }

    @Nonnull
    @Override
    public JetService getJet() {
        return client.getJet();
    }
}
