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

import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.partition.PartitioningStrategy;
import uk.org.hecaton.partition.strategy.StringPartitioningStrategy;
import uk.org.hecaton.spi.impl.executionservice.ExecutionService;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.impl.operationservice.impl.InvocationFuture;
import uk.org.hecaton.spi.impl.proxyservice.ProxyService;
import uk.org.hecaton.version.Version;

import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Abstract DistributedObject implementation. Useful to provide basic functionality.
 *
 * @param <S>
 */
public abstract class AbstractDistributedObject<S extends RemoteService> implements DistributedObject {

    protected static final PartitioningStrategy PARTITIONING_STRATEGY = StringPartitioningStrategy.INSTANCE;
    protected final Executor internalAsyncExecutor;

    private volatile NodeEngine nodeEngine;
    private volatile S service;

    protected AbstractDistributedObject(NodeEngine nodeEngine, S service) {
        this.nodeEngine = nodeEngine;
        this.service = service;
        this.internalAsyncExecutor = nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);
    }

    protected String getDistributedObjectName() {
        return getName();
    }

    protected Data getNameAsPartitionAwareData() {
        String name = getDistributedObjectName();
        return getNodeEngine().getSerializationService().toData(name, PARTITIONING_STRATEGY);
    }

    @Override
    public String getPartitionKey() {
        return StringPartitioningStrategy.getPartitionKey(getDistributedObjectName());
    }

    @Override
    public final void destroy() {
        // IMPORTANT NOTE: This method should NOT be called from client MessageTasks.

        if (preDestroy()) {
            NodeEngine engine = getNodeEngine();
            ProxyService proxyService = engine.getProxyService();
            UUID source = engine.getLocalMember().getUuid();
            proxyService.destroyDistributedObject(getServiceName(), getDistributedObjectName(), source);
            postDestroy();
        }
    }

    protected final Data toData(Object object) {
        return getNodeEngine().toData(object);
    }

    protected final <E> InvocationFuture<E> invokeOnPartition(Operation operation) {
        return getNodeEngine().getOperationService().invokeOnPartition(operation);
    }

    protected final int getPartitionId(Data key) {
        return getNodeEngine().getPartitionService().getPartitionId(key);
    }

    protected boolean preDestroy() {
        return true;
    }

    protected void postDestroy() {
    }

    /**
     * Gets the node engine.
     *
     * @return the node engine
     * @throws HecatonInstanceNotActiveException if NodeEngine not active or DistributedObject destroyed.
     */
    public final NodeEngine getNodeEngine() {
        final NodeEngine engine = nodeEngine;
        lifecycleCheck(engine);
        return engine;
    }

    private void lifecycleCheck(final NodeEngine engine) {
        if (engine == null || !engine.isRunning()) {
            throwNotActiveException();
        }
    }

    protected void throwNotActiveException() {
        throw new HecatonInstanceNotActiveException();
    }

    /**
     * Gets the Service of this AbstractDistributedObject.
     *
     * @return the Service of this AbstractDistributedObject
     * @throws HecatonInstanceNotActiveException if object is destroyed or HecatonInstance shutdown.
     */
    public final S getService() {
        final S s = service;
        if (s == null) {
            throw new HecatonInstanceNotActiveException();
        }
        return s;
    }

    /**
     * Gets the OperationService.
     *
     * @return the OperationService.
     * @throws HecatonInstanceNotActiveException if object is destroyed or HecatonInstance shutdown.
     */
    public final OperationService getOperationService() {
        return getNodeEngine().getOperationService();
    }

    public final void invalidate() {
        nodeEngine = null;
        service = null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractDistributedObject that = (AbstractDistributedObject) o;
        Object name = getDistributedObjectName();
        if (name != null ? !name.equals(that.getDistributedObjectName()) : that.getDistributedObjectName() != null) {
            return false;
        }

        String serviceName = getServiceName();
        if (serviceName != null ? !serviceName.equals(that.getServiceName()) : that.getServiceName() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getServiceName() != null ? getServiceName().hashCode() : 0;
        result = 31 * result + (getDistributedObjectName() != null ? getDistributedObjectName().hashCode() : 0);
        return result;
    }


    @Override
    public String toString() {
        return getClass().getName() + '{' + "service=" + getServiceName() + ", name=" + getName() + '}';
    }

    protected boolean isClusterVersionLessThan(Version version) {
        Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        return clusterVersion.isLessThan(version);
    }

    protected boolean isClusterVersionUnknownOrLessThan(Version version) {
        Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        return clusterVersion.isUnknownOrLessThan(version);
    }

    protected boolean isClusterVersionLessOrEqual(Version version) {
        Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        return clusterVersion.isLessOrEqual(version);
    }

    protected boolean isClusterVersionUnknownOrLessOrEqual(Version version) {
        Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        return clusterVersion.isUnknownOrLessOrEqual(version);
    }

    protected boolean isClusterVersionGreaterThan(Version version) {
        Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        return clusterVersion.isGreaterThan(version);
    }

    protected boolean isClusterVersionUnknownOrGreaterThan(Version version) {
        Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        return clusterVersion.isUnknownOrGreaterThan(version);
    }

    protected boolean isClusterVersionGreaterOrEqual(Version version) {
        Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        return clusterVersion.isGreaterOrEqual(version);
    }

    protected boolean isClusterVersionUnknownOrGreaterOrEqual(Version version) {
        Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        return clusterVersion.isUnknownOrGreaterOrEqual(version);
    }

    protected boolean isClusterVersionEqualTo(Version version) {
        Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        return clusterVersion.isEqualTo(version);
    }

    protected boolean isClusterVersionUnknown() {
        Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        return clusterVersion.isUnknown();
    }
}
