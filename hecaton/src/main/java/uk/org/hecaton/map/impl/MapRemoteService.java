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

package uk.org.hecaton.map.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.internal.loadprotection.LoadProtection;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.map.impl.proxy.NearCachedMapProxyImpl;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicyProvider;

import java.util.UUID;

import static uk.org.hecaton.internal.config.ConfigValidator.checkImmutableMapConfig;
import static uk.org.hecaton.internal.config.ConfigValidator.checkMapConfig;
import static uk.org.hecaton.internal.config.ConfigValidator.checkNearCacheConfig;

/**
 * Defines remote service behavior of map service.
 *
 * @see MapService
 */
class MapRemoteService implements RemoteService {

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;

    MapRemoteService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
    }

    @Override
    public DistributedObject createDistributedObject(String name, UUID source, boolean local) {
        Config config = nodeEngine.getConfig();
        MapConfig mapConfig = config.findMapConfig(name);
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();

        checkMapConfig(config, mapConfig, mergePolicyProvider
        );
        LoadProtection loadProtection = ((NodeEngineImpl) nodeEngine).getLoadProtection();
        if (loadProtection.isImmutable(mapConfig)) {
            checkImmutableMapConfig(mapConfig);
            mapConfig = loadProtection.withImmutableMapNearCache(mapConfig);
        }

        if (mapConfig.isNearCacheEnabled()) {
            checkNearCacheConfig(name, mapConfig.getNearCacheConfig(), config.getNativeMemoryConfig(), false);
            return new NearCachedMapProxyImpl(name, mapServiceContext.getService(), nodeEngine, mapConfig);
        } else {
            return new MapProxyImpl(name, mapServiceContext.getService(), nodeEngine, mapConfig);
        }
    }

    @Override
    public void destroyDistributedObject(String name, boolean local) {
        mapServiceContext.destroyMap(name);
    }
}
