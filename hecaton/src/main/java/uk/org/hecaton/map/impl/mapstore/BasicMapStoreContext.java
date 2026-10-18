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

package uk.org.hecaton.map.impl.mapstore;

import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.partition.PartitioningStrategy;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.MapStoreWrapper;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.util.IterableUtil;

import java.util.Properties;

import static uk.org.hecaton.map.impl.mapstore.MapStoreManagers.createWriteBehindManager;
import static uk.org.hecaton.map.impl.mapstore.MapStoreManagers.createWriteThroughManager;
import static uk.org.hecaton.map.impl.mapstore.StoreConstructor.createStore;

/**
 * Default impl. of {@link uk.org.hecaton.map.impl.mapstore.MapStoreContext}
 * One instance is created per map.
 */
final class BasicMapStoreContext implements MapStoreContext {

    private String mapName;

    private MapStoreManager mapStoreManager;

    private MapStoreWrapper storeWrapper;

    private MapServiceContext mapServiceContext;

    private MapStoreConfig mapStoreConfig;

    private BasicMapStoreContext() {
    }

    @Override
    public void start() {
        mapStoreManager.start();
    }

    @Override
    public void stop() {
        mapStoreManager.stop();
    }

    @Override
    public boolean isWriteBehindMapStoreEnabled() {
        final MapStoreConfig mapStoreConfig = getMapStoreConfig();
        return mapStoreConfig != null && mapStoreConfig.isEnabled()
                && mapStoreConfig.getWriteDelaySeconds() > 0;
    }

    @Override
    public boolean isMapLoader() {
        return storeWrapper.isMapLoader();
    }

    @Override
    public SerializationService getSerializationService() {
        return mapServiceContext.getNodeEngine().getSerializationService();
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return mapServiceContext.getNodeEngine().getLogger(clazz);
    }

    @Override
    public String getMapName() {
        return mapName;
    }

    @Override
    public MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }

    @Override
    public MapStoreConfig getMapStoreConfig() {
        return mapStoreConfig;
    }

    @Override
    public MapStoreManager getMapStoreManager() {
        return mapStoreManager;
    }

    @Override
    public MapStoreWrapper getMapStoreWrapper() {
        return storeWrapper;
    }

    static MapStoreContext create(MapContainer mapContainer) {
        final BasicMapStoreContext context = new BasicMapStoreContext();
        final String mapName = mapContainer.getName();
        final MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final PartitioningStrategy partitioningStrategy = mapContainer.getPartitioningStrategy();
        final MapConfig mapConfig = mapContainer.getMapConfig();
        final MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        final ClassLoader classLoader = NamespaceUtil.getClassLoaderForNamespace(nodeEngine, mapConfig.getUserCodeNamespace());
        // create store.
        // Despite passing the Namespace aware class loader, we still need to wrap the entire call in
        //  Namespace awareness separately as the class loader is only for instantiation, and execution
        //  of the implementation takes place in this call as well.
        final Object store = NamespaceUtil.callWithNamespace(nodeEngine, mapConfig.getUserCodeNamespace(),
                () -> createStore(mapName, mapStoreConfig, classLoader));
        final MapStoreWrapper storeWrapper = new MapStoreWrapper(nodeEngine, mapName, store, mapConfig.getUserCodeNamespace());
        storeWrapper.instrument(nodeEngine);

        context.setMapName(mapName);
        context.setMapStoreConfig(mapStoreConfig);
        context.setPartitioningStrategy(partitioningStrategy);
        context.setMapServiceContext(mapServiceContext);
        context.setStoreWrapper(storeWrapper);

        final MapStoreManager mapStoreManager = createMapStoreManager(context);
        context.setMapStoreManager(mapStoreManager);

        // todo this is user code. it may also block map store creation.
        callLifecycleSupportInit(context);

        return context;
    }

    private static MapStoreManager createMapStoreManager(MapStoreContext mapStoreContext) {
        final MapStoreConfig mapStoreConfig = mapStoreContext.getMapStoreConfig();
        if (isWriteBehindMapStoreEnabled(mapStoreConfig)) {
            return createWriteBehindManager(mapStoreContext);
        }
        return createWriteThroughManager(mapStoreContext);
    }

    private static boolean isWriteBehindMapStoreEnabled(MapStoreConfig mapStoreConfig) {
        return mapStoreConfig != null && mapStoreConfig.isEnabled()
                && mapStoreConfig.getWriteDelaySeconds() > 0;
    }

    private static void callLifecycleSupportInit(MapStoreContext mapStoreContext) {
        final MapStoreWrapper mapStoreWrapper = mapStoreContext.getMapStoreWrapper();
        final MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final HecatonInstance hecatonInstance = nodeEngine.getHecatonInstance();
        final MapStoreConfig mapStoreConfig = mapStoreContext.getMapStoreConfig();
        final Properties properties = mapStoreConfig.getProperties();
        final String mapName = mapStoreContext.getMapName();

        mapStoreWrapper.init(hecatonInstance, properties, mapName);
    }

    @Override
    public Iterable<Object> loadAllKeys() {
        return IterableUtil.nullToEmpty(storeWrapper.loadAllKeys());
    }

    void setMapStoreManager(MapStoreManager mapStoreManager) {
        this.mapStoreManager = mapStoreManager;
    }

    void setStoreWrapper(MapStoreWrapper storeWrapper) {
        this.storeWrapper = storeWrapper;
    }

    void setMapServiceContext(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    void setMapName(String mapName) {
        this.mapName = mapName;
    }

    void setPartitioningStrategy(PartitioningStrategy partitioningStrategy) {
    }

    void setMapStoreConfig(MapStoreConfig mapStoreConfig) {
        this.mapStoreConfig = mapStoreConfig;
    }
}
