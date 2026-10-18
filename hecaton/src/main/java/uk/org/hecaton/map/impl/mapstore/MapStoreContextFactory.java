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
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.MapStoreWrapper;
import uk.org.hecaton.internal.serialization.SerializationService;

import java.util.Collections;

import static uk.org.hecaton.map.impl.mapstore.MapStoreManagers.emptyMapStoreManager;

/**
 * A factory which creates {@link uk.org.hecaton.map.impl.mapstore.MapStoreContext} objects
 * according to {@link uk.org.hecaton.config.MapStoreConfig}.
 */
public final class MapStoreContextFactory {

    private static final MapStoreContext EMPTY_MAP_STORE_CONTEXT = new EmptyMapStoreContext();

    private MapStoreContextFactory() {
    }

    public static MapStoreContext createMapStoreContext(MapContainer mapContainer) {
        final MapConfig mapConfig = mapContainer.getMapConfig();
        final MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        if (mapStoreConfig == null || !mapStoreConfig.isEnabled()) {
            return EMPTY_MAP_STORE_CONTEXT;
        }
        return BasicMapStoreContext.create(mapContainer);
    }

    private static final class EmptyMapStoreContext implements MapStoreContext {

        @Override
        public MapStoreManager getMapStoreManager() {
            return emptyMapStoreManager();
        }

        @Override
        public MapStoreWrapper getMapStoreWrapper() {
            // keep it null. do not throw exception.
            return null;
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public boolean isWriteBehindMapStoreEnabled() {
            return false;
        }

        @Override
        public SerializationService getSerializationService() {
            throw new UnsupportedOperationException("This method must not be called. No defined map store exists.");
        }

        @Override
        public ILogger getLogger(Class clazz) {
            throw new UnsupportedOperationException("This method must not be called. No defined map store exists.");
        }

        @Override
        public String getMapName() {
            throw new UnsupportedOperationException("This method must not be called. No defined map store exists.");
        }

        @Override
        public MapServiceContext getMapServiceContext() {
            throw new UnsupportedOperationException("This method must not be called. No defined map store exists.");
        }

        @Override
        public MapStoreConfig getMapStoreConfig() {
            throw new UnsupportedOperationException("This method must not be called. No defined map store exists.");
        }

        @Override
        public Iterable<Object> loadAllKeys() {
            return Collections.emptyList();
        }

        @Override
        public boolean isMapLoader() {
            return false;
        }
    }
}
