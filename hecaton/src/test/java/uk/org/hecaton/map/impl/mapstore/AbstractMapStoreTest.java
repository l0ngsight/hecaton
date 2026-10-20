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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.test.HecatonTestSupport;

import java.util.concurrent.ExecutionException;

public abstract class AbstractMapStoreTest extends HecatonTestSupport {

    public Config newConfig(Object storeImpl, int writeDelaySeconds) {
        return newConfig("default", storeImpl, writeDelaySeconds, MapStoreConfig.InitialLoadMode.LAZY);
    }

    public Config newConfig(Object storeImpl, int writeDelaySeconds, MapStoreConfig.InitialLoadMode loadMode) {
        return newConfig("default", storeImpl, writeDelaySeconds, loadMode);
    }

    public Config newConfig(String mapName, Object storeImpl, int writeDelaySeconds) {
        return newConfig(mapName, storeImpl, writeDelaySeconds, MapStoreConfig.InitialLoadMode.LAZY);
    }

    public Config newConfig(String mapName, Object storeImpl, int writeDelaySeconds,
                            MapStoreConfig.InitialLoadMode loadMode) {
        Config config = getConfig();
        config.getMetricsConfig().setEnabled(false);
        MapConfig mapConfig = config.getMapConfig(mapName);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(storeImpl);
        mapStoreConfig.setWriteDelaySeconds(writeDelaySeconds);
        mapStoreConfig.setInitialLoadMode(loadMode);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        return config;
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfigWithoutJetAndMetrics();
    }

    protected static <K, V> void flush(IMap<K, V> map, boolean async) {
        if (async) {
            try {
                ((MapProxyImpl<K, V>) map).flushAsync().get();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            map.flush();
        }
    }
}
