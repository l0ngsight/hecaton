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

package uk.org.hecaton.osgi.impl;

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
import uk.org.hecaton.internal.util.StringUtil;
import uk.org.hecaton.jet.JetService;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.multimap.MultiMap;
import uk.org.hecaton.osgi.HecatonOSGiInstance;
import uk.org.hecaton.osgi.HecatonOSGiService;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link uk.org.hecaton.osgi.HecatonOSGiInstance} implementation
 * as proxy of delegated {@link uk.org.hecaton.core.HecatonInstance} for getting from OSGi service.
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity", "MethodCount"})
class HecatonOSGiInstanceImpl
        implements HecatonOSGiInstance {

    private final HecatonInstance delegatedInstance;
    private final HecatonOSGiService ownerService;

    HecatonOSGiInstanceImpl(HecatonInstance delegatedInstance,
                              HecatonOSGiService ownerService) {
        this.delegatedInstance = delegatedInstance;
        this.ownerService = ownerService;
    }

    @Nonnull
    @Override
    public String getName() {
        return delegatedInstance.getName();
    }

    @Nonnull
    @Override
    public <E> IQueue<E> getQueue(@Nonnull String name) {
        return delegatedInstance.getQueue(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getTopic(@Nonnull String name) {
        return delegatedInstance.getTopic(name);
    }

    @Nonnull
    @Override
    public <E> ISet<E> getSet(@Nonnull String name) {
        return delegatedInstance.getSet(name);
    }

    @Nonnull
    @Override
    public <E> IList<E> getList(@Nonnull String name) {
        return delegatedInstance.getList(name);
    }

    @Nonnull
    @Override
    public <K, V> IMap<K, V> getMap(@Nonnull String name) {
        return delegatedInstance.getMap(name);
    }

    @Nonnull
    @Override
    public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
        return delegatedInstance.getReplicatedMap(name);
    }

    @Nonnull
    @Override
    public <K, V> MultiMap<K, V> getMultiMap(@Nonnull String name) {
        return delegatedInstance.getMultiMap(name);
    }

    @Nonnull
    @Override
    public <E> Ringbuffer<E> getRingbuffer(@Nonnull String name) {
        return delegatedInstance.getRingbuffer(name);
    }

    @Nonnull
    @Override
    public <E> ITopic<E> getReliableTopic(@Nonnull String name) {
        return delegatedInstance.getReliableTopic(name);
    }

    @Override
    public ICacheManager getCacheManager() {
        return delegatedInstance.getCacheManager();
    }

    @Nonnull
    @Override
    public Cluster getCluster() {
        return delegatedInstance.getCluster();
    }

    @Nonnull
    @Override
    public Endpoint getLocalEndpoint() {
        return delegatedInstance.getLocalEndpoint();
    }

    @Nonnull
    @Override
    public IExecutorService getExecutorService(@Nonnull String name) {
        return delegatedInstance.getExecutorService(name);
    }

    @Nonnull
    @Override
    public DurableExecutorService getDurableExecutorService(@Nonnull String name) {
        return delegatedInstance.getDurableExecutorService(name);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionalTask<T> task) throws TransactionException {
        return delegatedInstance.executeTransaction(task);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionOptions options,
                                    @Nonnull TransactionalTask<T> task) throws TransactionException {
        return delegatedInstance.executeTransaction(options, task);
    }

    @Override
    public TransactionContext newTransactionContext() {
        return delegatedInstance.newTransactionContext();
    }

    @Override
    public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
        return delegatedInstance.newTransactionContext(options);
    }

    @Nonnull
    @Override
    public FlakeIdGenerator getFlakeIdGenerator(@Nonnull String name) {
        return delegatedInstance.getFlakeIdGenerator(name);
    }

    @Override
    public Collection<DistributedObject> getDistributedObjects() {
        return delegatedInstance.getDistributedObjects();
    }

    @Override
    public UUID addDistributedObjectListener(@Nonnull DistributedObjectListener distributedObjectListener) {
        return delegatedInstance.addDistributedObjectListener(distributedObjectListener);
    }

    @Override
    public boolean removeDistributedObjectListener(@Nonnull UUID registrationId) {
        return delegatedInstance.removeDistributedObjectListener(registrationId);
    }

    @Nonnull
    @Override
    public Config getConfig() {
        return delegatedInstance.getConfig();
    }

    @Nonnull
    @Override
    public PartitionService getPartitionService() {
        return delegatedInstance.getPartitionService();
    }

    @Nonnull
    @Override
    public SplitBrainProtectionService getSplitBrainProtectionService() {
        return delegatedInstance.getSplitBrainProtectionService();
    }

    @Nonnull
    @Override
    public ClientService getClientService() {
        return delegatedInstance.getClientService();
    }

    @Nonnull
    @Override
    public LoggingService getLoggingService() {
        return delegatedInstance.getLoggingService();
    }

    @Nonnull
    @Override
    public LifecycleService getLifecycleService() {
        return delegatedInstance.getLifecycleService();
    }

    @Nonnull
    @Override
    public <T extends DistributedObject> T getDistributedObject(@Nonnull String serviceName, @Nonnull String name) {
        return delegatedInstance.getDistributedObject(serviceName, name);
    }

    @Nonnull
    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return delegatedInstance.getUserContext();
    }

    @Nonnull
    @Override
    public HecatonXAResource getXAResource() {
        return delegatedInstance.getXAResource();
    }

    @Nonnull
    @Override
    public CardinalityEstimator getCardinalityEstimator(@Nonnull String name) {
        return delegatedInstance.getCardinalityEstimator(name);
    }

    @Nonnull
    @Override
    public PNCounter getPNCounter(@Nonnull String name) {
        return delegatedInstance.getPNCounter(name);
    }

    @Nonnull
    @Override
    public IScheduledExecutorService getScheduledExecutorService(@Nonnull String name) {
        return delegatedInstance.getScheduledExecutorService(name);
    }

    @Nonnull
    @Override
    public CPSubsystem getCPSubsystem() {
        return delegatedInstance.getCPSubsystem();
    }

    @Nonnull
    @Override
    public SqlService getSql() {
        return delegatedInstance.getSql();
    }

    @Nonnull
    @Override
    public DataConnectionService getDataConnectionService() {
        return delegatedInstance.getDataConnectionService();
    }

    @Nonnull
    @Override
    public JetService getJet() {
        return delegatedInstance.getJet();
    }

    @Override
    public void shutdown() {
        delegatedInstance.shutdown();
    }

    @Override
    public HecatonInstance getDelegatedInstance() {
        return delegatedInstance;
    }

    @Override
    public HecatonOSGiService getOwnerService() {
        return ownerService;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HecatonOSGiInstanceImpl that = (HecatonOSGiInstanceImpl) o;

        if (!delegatedInstance.equals(that.delegatedInstance)) {
            return false;
        }
        if (!ownerService.equals(that.ownerService)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = ownerService.hashCode();
        result = 31 * result + delegatedInstance.hashCode();
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("HecatonOSGiInstanceImpl");
        sb.append("{delegatedInstance='").append(delegatedInstance).append('\'');
        Config config = getConfig();
        if (!StringUtil.isNullOrEmpty(config.getClusterName())) {
            sb.append(", clusterName=").append(config.getClusterName());
        }
        sb.append(", ownerServiceId=").append(ownerService.getId());
        sb.append('}');
        return sb.toString();
    }

}
