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
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.DistributedObjectListener;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.core.ICacheManager;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.core.LifecycleService;
import uk.org.hecaton.cp.CPSubsystem;
import uk.org.hecaton.crdt.pncounter.PNCounter;
import uk.org.hecaton.dataconnection.DataConnectionService;
import uk.org.hecaton.durableexecutor.DurableExecutorService;
import uk.org.hecaton.flakeidgen.FlakeIdGenerator;
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
 * A proxy around the actual HecatonInstanceImpl. This class serves 2 purposes:
 * <ol>
 * <li>
 * if the HecatonInstance is shut down, the reference to the original HecatonInstanceImpl is nulled and
 * this HecatonInstanceImpl and all its dependencies can be GCed. If the HecatonInstanceImpl would
 * be exposed directly, it could still retain unusable objects due to its not-null fields.</li>
 * <li>
 * it provides a barrier for accessing the HecatonInstanceImpl internals. Otherwise a simple cast to HecatonInstanceImpl
 * would be sufficient but now a bit of reflection is needed to get there.
 * </li>
 * </ol>
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public final class HecatonInstanceProxy implements HecatonInstance, SerializationServiceSupport {

    protected volatile HecatonInstanceImpl original;

    private final String name;

    protected HecatonInstanceProxy(HecatonInstanceImpl original) {
        this.original = original;
        name = original.getName();
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        return getOriginal().getMap(name);
    }

    @Nonnull
    @Override
    public <E> IQueue<E> getQueue(@Nonnull String name) {
        return getOriginal().getQueue(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getTopic(@Nonnull String name) {
        return getOriginal().getTopic(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getReliableTopic(@Nonnull String name) {
        return getOriginal().getReliableTopic(name);
    }

    @Nonnull
    @Override
    public <E> ISet<E> getSet(@Nonnull String name) {
        return getOriginal().getSet(name);
    }

    @Nonnull
    @Override
    public <E> IList<E> getList(@Nonnull String name) {
        return getOriginal().getList(name);
    }

    @Nonnull
    @Override
    public <K, V> MultiMap<K, V> getMultiMap(@Nonnull String name) {
        return getOriginal().getMultiMap(name);
    }

    @Nonnull
    @Override
    public <E> Ringbuffer<E> getRingbuffer(@Nonnull String name) {
        return getOriginal().getRingbuffer(name);
    }

    @Nonnull
    @Override
    public IExecutorService getExecutorService(@Nonnull String name) {
        return getOriginal().getExecutorService(name);
    }

    @Nonnull
    @Override
    public DurableExecutorService getDurableExecutorService(@Nonnull String name) {
        return getOriginal().getDurableExecutorService(name);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionalTask<T> task) throws TransactionException {
        return getOriginal().executeTransaction(task);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionOptions options,
                                    @Nonnull TransactionalTask<T> task) throws TransactionException {
        return getOriginal().executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return getOriginal().newTransactionContext();
    }

    @Override
    public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
        return getOriginal().newTransactionContext(options);
    }

    @Nonnull
    @Override
    public FlakeIdGenerator getFlakeIdGenerator(@Nonnull String name) {
        return getOriginal().getFlakeIdGenerator(name);
    }

    @Nonnull
    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return getOriginal().getReplicatedMap(name);
    }

    @Override
    public ICacheManager getCacheManager() {
        return getOriginal().getCacheManager();
    }

    @Nonnull
    @Override
    public Cluster getCluster() {
        return getOriginal().getCluster();
    }

    @Nonnull
    @Override
    public Member getLocalEndpoint() {
        return getOriginal().getLocalEndpoint();
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        return getOriginal().getDistributedObjects();
    }

    @Nonnull
    @Override
    public Config getConfig() {
        return getOriginal().getConfig();
    }

    @Nonnull
    @Override
    public PartitionService getPartitionService() {
        return getOriginal().getPartitionService();
    }

    @Nonnull
    @Override
    public SplitBrainProtectionService getSplitBrainProtectionService() {
        return getOriginal().getSplitBrainProtectionService();
    }

    @Nonnull
    @Override
    public ClientService getClientService() {
        return getOriginal().getClientService();
    }

    @Nonnull
    @Override
    public LoggingService getLoggingService() {
        return getOriginal().getLoggingService();
    }

    @Nonnull
    @Override
    public LifecycleService getLifecycleService() {
        final HecatonInstanceImpl hz = original;
        return hz != null ? hz.getLifecycleService() : new TerminatedLifecycleService();
    }

    @Nonnull
    @Override
    public <T extends DistributedObject> T getDistributedObject(@Nonnull String serviceName, @Nonnull String name) {
        return getOriginal().getDistributedObject(serviceName, name);
    }

    @Override
    public UUID addDistributedObjectListener(@Nonnull DistributedObjectListener distributedObjectListener) {
        return getOriginal().addDistributedObjectListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(@Nonnull UUID registrationId) {
        return getOriginal().removeDistributedObjectListener(registrationId);
    }

    @Nonnull
    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return getOriginal().getUserContext();
    }

    @Nonnull
    @Override
    public HecatonXAResource getXAResource() {
        return getOriginal().getXAResource();
    }

    @Nonnull
    @Override
    public CardinalityEstimator getCardinalityEstimator(@Nonnull String name) {
        return getOriginal().getCardinalityEstimator(name);
    }

    @Nonnull
    @Override
    public PNCounter getPNCounter(@Nonnull String name) {
        return getOriginal().getPNCounter(name);
    }

    @Nonnull
    @Override
    public IScheduledExecutorService getScheduledExecutorService(@Nonnull String name) {
        return getOriginal().getScheduledExecutorService(name);
    }

    @Nonnull
    @Override
    public CPSubsystem getCPSubsystem() {
        return getOriginal().getCPSubsystem();
    }

    @Nonnull
    @Override
    public SqlService getSql() {
        return getOriginal().getSql();
    }

    @Nonnull
    @Override
    public DataConnectionService getDataConnectionService() {
        return getOriginal().getDataConnectionService();
    }

    @Nonnull
    @Override
    public JetService getJet() {
        return getOriginal().getJet();
    }

    @Override
    public void shutdown() {
        getLifecycleService().shutdown();
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return getOriginal().getSerializationService();
    }

    public HecatonInstanceImpl getOriginal() {
        final HecatonInstanceImpl hecatonInstance = original;
        if (hecatonInstance == null) {
            throw new HecatonInstanceNotActiveException();
        }
        return hecatonInstance;
    }

    @Override
    public String toString() {
        final HecatonInstanceImpl hecatonInstance = original;
        if (hecatonInstance != null) {
            return hecatonInstance.toString();
        }
        return "HecatonInstance {NOT ACTIVE}";
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HecatonInstance that)) {
            return false;
        }

        return !(name != null ? !name.equals(that.getName()) : that.getName() != null);
    }
}


