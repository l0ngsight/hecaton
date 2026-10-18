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

package uk.org.hecaton.client.impl.spi;

import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.client.cache.impl.nearcache.invalidation.ClientCacheInvalidationMetaDataFetcher;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.ClientExtension;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.connection.ClientConnectionManager;
import uk.org.hecaton.client.map.impl.querycache.ClientQueryCacheContext;
import uk.org.hecaton.client.map.impl.nearcache.invalidation.ClientMapInvalidationMetaDataFetcher;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleService;
import uk.org.hecaton.internal.nearcache.NearCacheManager;
import uk.org.hecaton.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MinimalPartitionService;
import uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.spi.impl.executionservice.TaskScheduler;
import uk.org.hecaton.spi.properties.HecatonProperties;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static java.lang.String.format;

/**
 * Context holding all the required services, managers and the configuration for a Hecaton client.
 */
public class ClientContext {

    private final String name;
    private final ProxyManager proxyManager;
    private final ClientConfig clientConfig;
    private final LoggingService loggingService;
    private final HecatonProperties properties;
    private final ClientExtension clientExtension;
    private final LifecycleService lifecycleService;
    private final ClientClusterService clusterService;
    private final ClientListenerService listenerService;
    private final TaskScheduler taskScheduler;
    private final ClientPartitionService partitionService;
    private final ClientQueryCacheContext queryCacheContext;
    private final ClientInvocationService invocationService;
    private final MinimalPartitionService minimalPartitionService;
    private final ClientConnectionManager clientConnectionManager;
    private final InternalSerializationService serializationService;
    private final ClientTransactionManagerService transactionManager;
    private final ConcurrentMap<String, RepairingTask> repairingTasks = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, NearCacheManager> nearCacheManagers = new ConcurrentHashMap<>();

    public ClientContext(HecatonClientInstanceImpl client) {
        this.name = client.getName();
        this.serializationService = client.getSerializationService();
        this.clusterService = client.getClientClusterService();
        this.partitionService = client.getClientPartitionService();
        this.invocationService = client.getInvocationService();
        this.taskScheduler = client.getTaskScheduler();
        this.listenerService = client.getListenerService();
        this.clientConnectionManager = client.getConnectionManager();
        this.lifecycleService = client.getLifecycleService();
        this.proxyManager = client.getProxyManager();
        this.clientConfig = client.getClientConfig();
        this.transactionManager = client.getTransactionManager();
        this.loggingService = client.getLoggingService();
        this.properties = client.getProperties();
        this.minimalPartitionService = new ClientMinimalPartitionService();
        this.queryCacheContext = client.getQueryCacheContext();
        this.clientExtension = client.getClientExtension();

        registerDisposalTasksTo(client);
    }

    private void registerDisposalTasksTo(HecatonClientInstanceImpl client) {
        client.disposeOnClusterChange(() -> nearCacheManagers.values().forEach(NearCacheManager::clearAllNearCaches));
        client.disposeOnClientShutdown(() -> nearCacheManagers.values().forEach(NearCacheManager::destroyAllNearCaches));
    }

    public ClientQueryCacheContext getQueryCacheContext() {
        return queryCacheContext;
    }

    public NearCacheManager getNearCacheManager(String serviceName) {
        return getOrPutIfAbsent(nearCacheManagers, serviceName,
                anyArg -> clientExtension.createNearCacheManager());
    }

    public ConcurrentMap<String, NearCacheManager> getNearCacheManagers() {
        return nearCacheManagers;
    }

    public RepairingTask getRepairingTask(String serviceName) {
        return getOrPutIfAbsent(repairingTasks, serviceName, name -> {
            InvalidationMetaDataFetcher invalidationMetaDataFetcher = newMetaDataFetcher(serviceName);
            ILogger logger = loggingService.getLogger(RepairingTask.class);
            return new RepairingTask(properties, invalidationMetaDataFetcher, taskScheduler, serializationService,
                    minimalPartitionService,
                    clientConnectionManager.getClientUuid(), logger);
        });
    }

    private InvalidationMetaDataFetcher newMetaDataFetcher(String serviceName) {
        if (MapService.SERVICE_NAME.equals(serviceName)) {
            return new ClientMapInvalidationMetaDataFetcher(this);
        }

        if (CacheService.SERVICE_NAME.equals(serviceName)) {
            return new ClientCacheInvalidationMetaDataFetcher(this);
        }

        throw new IllegalArgumentException(format("%s is not a known service-name to fetch metadata for", serviceName));
    }

    public String getName() {
        return name;
    }

    /**
     * Client side implementation of {@link MinimalPartitionService}
     */
    private class ClientMinimalPartitionService implements MinimalPartitionService {

        @Override
        public int getPartitionId(Data key) {
            return partitionService.getPartitionId(key);
        }

        @Override
        public int getPartitionId(Object key) {
            return partitionService.getPartitionId(key);
        }

        @Override
        public int getPartitionCount() {
            return partitionService.getPartitionCount();
        }
    }

    public HecatonInstance getHecatonInstance() {
        return proxyManager.getHecatonInstance();
    }

    public InternalSerializationService getSerializationService() {
        return serializationService;
    }

    public ClientClusterService getClusterService() {
        return clusterService;
    }

    public ClientPartitionService getPartitionService() {
        return partitionService;
    }

    public ClientTransactionManagerService getTransactionManager() {
        return transactionManager;
    }

    public TaskScheduler getTaskScheduler() {
        return taskScheduler;
    }

    public ClientListenerService getListenerService() {
        return listenerService;
    }

    public LoggingService getLoggingService() {
        return loggingService;
    }

    public ClientInvocationService getInvocationService() {
        return invocationService;
    }

    public ClientConnectionManager getConnectionManager() {
        return clientConnectionManager;
    }

    public LifecycleService getLifecycleService() {
        return lifecycleService;
    }

    public ProxyManager getProxyManager() {
        return proxyManager;
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public boolean isActive() {
        return getHecatonInstance().getLifecycleService().isRunning();
    }
}
