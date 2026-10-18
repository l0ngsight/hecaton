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

package uk.org.hecaton.spi.impl.tenantcontrol.impl;

import uk.org.hecaton.core.DistributedObjectEvent;
import uk.org.hecaton.core.DistributedObjectListener;
import uk.org.hecaton.internal.cluster.ClusterVersionListener;
import uk.org.hecaton.internal.services.PreJoinAwareService;
import uk.org.hecaton.internal.util.ExceptionUtil;
import uk.org.hecaton.internal.util.MapUtil;
import uk.org.hecaton.internal.util.ServiceLoader;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.tenantcontrol.TenantControl;
import uk.org.hecaton.spi.tenantcontrol.TenantControlFactory;
import uk.org.hecaton.version.Version;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static uk.org.hecaton.internal.util.InvocationUtil.invokeOnStableClusterSerial;
import static uk.org.hecaton.spi.tenantcontrol.TenantControlFactory.NOOP_TENANT_CONTROL_FACTORY;

/**
 * The tenant control service is responsible for handling the lifecycle of
 * {@link TenantControl} instances and their replication across the cluster.
 * <p>
 * Each tenant control instance is bound to a single distributed object.
 */
public class TenantControlServiceImpl
        implements ClusterVersionListener, DistributedObjectListener, PreJoinAwareService<TenantControlReplicationOperation> {
    public static final String SERVICE_NAME = "hz:impl:tenantControlService";
    /**
     * The number of retries for invocations replicating {@link TenantControl}
     * instances across the cluster.
     */
    private static final int MAX_RETRIES = 100;
    /**
     * The factory ID of the tenant control service which will be loaded via
     * service loading.
     */
    private static final String TENANT_CONTROL_FACTORY = "uk.org.hecaton.spi.tenantcontrol.TenantControlFactory";

    private final TenantControlFactory tenantControlFactory;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    /**
     * Tenant control instances, grouped by object name and distributed service
     * name.
     */
    private final ConcurrentMap<String, ConcurrentMap<String, TenantControl>>
            tenantControlMap = MapUtil.createConcurrentHashMap(1);

    public TenantControlServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(TenantControlServiceImpl.class.getName());
        this.tenantControlFactory = initTenantControlFactory(nodeEngine.getConfigClassLoader());
        nodeEngine.getProxyService().addProxyListener(this);
    }

    /**
     * Returns the tenant control for the specified distributed object, or
     * {@code null} if it does not exist.
     *
     * @param serviceName the distributed object service
     * @param objectName  the distributed object name
     * @return the tenant control for the specific
     */
    public @Nullable
    TenantControl getTenantControl(@Nonnull String serviceName,
                                   @Nonnull String objectName) {
        if (!isTenantControlEnabled()) {
            return TenantControl.NOOP_TENANT_CONTROL;
        }
        ConcurrentMap<String, TenantControl> objectTenantControl = tenantControlMap.get(serviceName);
        return objectTenantControl != null ? objectTenantControl.get(objectName) : null;
    }

    /**
     * Adds the provided tenant control instance which belongs to the provided
     * distributed object.
     *
     * @param serviceName   the distributed object service
     * @param objectName    the distributed object name
     * @param tenantControl the tenant control instance for the provided distributed object
     */
    public void appendTenantControl(@Nonnull String serviceName,
                                    @Nonnull String objectName,
                                    @Nonnull TenantControl tenantControl) {
        tenantControlMap.computeIfAbsent(serviceName, name -> new ConcurrentHashMap<>())
                        .putIfAbsent(objectName, tenantControl);
    }

    /**
     * Returns the factory for creating tenant control objects.
     */
    public TenantControlFactory getTenantControlFactory() {
        return tenantControlFactory;
    }

    /**
     * Creates a new {@link TenantControl} and propagates it to other members
     * in the cluster. The tenant control is created for a single distributed
     * object which is defined by a service and object name.
     * This method must be invoked on the application thread to properly capture
     * the appropriate tenant context.
     *
     * @param serviceName the distributed service name
     * @param objectName  the distributed object name
     * @return the created tenant control
     */
    public TenantControl initializeTenantControl(@Nonnull String serviceName, @Nonnull String objectName) {
        if (!isTenantControlEnabled()) {
            return TenantControl.NOOP_TENANT_CONTROL;
        }

        TenantControl tenantControl = tenantControlFactory.saveCurrentTenant();
        appendTenantControl(serviceName, objectName, tenantControl);

        try {
            invokeOnStableClusterSerial(
                    nodeEngine,
                    () -> new TenantControlReplicationOperation(serviceName, objectName, tenantControl),
                    MAX_RETRIES).get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return tenantControl;
    }

    @Override
    public void onClusterVersionChange(Version newVersion) {
        if (isTenantControlEnabled()) {
            // async, we should not block transaction
            InternalCompletableFuture<Object> future = invokeOnStableClusterSerial(nodeEngine,
                    () -> new TenantControlReplicationOperation(tenantControlMap), MAX_RETRIES);
            future.whenCompleteAsync((v, t) -> {
                if (t != null) {
                    logger.warning("Failed to propagate tenant control", t);
                }
            }, CALLER_RUNS);
        }
    }

    @Override
    public void distributedObjectCreated(DistributedObjectEvent event) {
        // tenant control creation is handled synchronously while creating the proxy
    }

    @Override
    public void distributedObjectDestroyed(DistributedObjectEvent event) {
        // asynchronous to proxy.destroy();

        ConcurrentMap<String, TenantControl> serviceMap = tenantControlMap.get(event.getServiceName());
        if (serviceMap == null) {
            // race between create and destroy
            return;
        }
        TenantControl tc = serviceMap.remove(event.getObjectName().toString());
        if (tc == null) {
            // race between create and destroy
            return;
        }
        tc.unregisterObject();
    }

    /**
     * Creates and returns a {@link TenantControlFactory} based on the configuration.
     *
     * @param configClassLoader the classloader to use when looking for the tenant control factory
     *                          implementation
     */
    private TenantControlFactory initTenantControlFactory(ClassLoader configClassLoader) {
        TenantControlFactory factory = null;
        try {
            factory = ServiceLoader.load(TenantControlFactory.class, TENANT_CONTROL_FACTORY, configClassLoader);
        } catch (Exception e) {
            if (logger.isFinestEnabled()) {
                logger.finest("Could not load service provider for TenantControl", e);
            }
        }
        return factory != null ? factory : NOOP_TENANT_CONTROL_FACTORY;
    }

    /**
     * Returns {@code true} if tenant control is enabled.
     */
    public boolean isTenantControlEnabled() {
        return tenantControlFactory != NOOP_TENANT_CONTROL_FACTORY;
    }

    @Override
    public TenantControlReplicationOperation getPreJoinOperation() {
        return isTenantControlEnabled()
                ? new TenantControlReplicationOperation(tenantControlMap)
                : null;
    }
}
