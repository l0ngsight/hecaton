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

package uk.org.hecaton.internal.management.operation;

import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.internal.config.MapConfigReadOnly;
import uk.org.hecaton.internal.util.MapUtil;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.spi.impl.operationservice.AbstractLocalOperation;

import java.util.Map;

/**
 * Operation to fetch Map configuration.
 */
public class GetMapConfigOperation
        extends AbstractLocalOperation {

    private final String mapName;

    private MapConfig mapConfig;

    public GetMapConfigOperation(String mapName) {
        this.mapName = mapName;
    }

    @Override
    public void run() throws Exception {
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        mapConfig = mapContainer.getMapConfig();
        mapConfig = addIndexConfig(mapConfig, mapContainer.getIndexDefinitions());
    }

    private static MapConfig addIndexConfig(MapConfig mapConfig,
                                            Map<String, IndexConfig> indexDefinitions) {
        // 1. If there is no index-config  to add
        if (MapUtil.isNullOrEmpty(indexDefinitions)) {
            return mapConfig;
        }

        // 2. We have index-configs to add
        MapConfig mapConfigWithIndexes = new MapConfig(mapConfig);
        // first clear all existing index-configs
        // here, not to have duplicate index-configs
        mapConfigWithIndexes.getIndexConfigs().clear();
        for (IndexConfig indexConfig : indexDefinitions.values()) {
            mapConfigWithIndexes.addIndexConfig(indexConfig);
        }
        return new MapConfigReadOnly(mapConfigWithIndexes);
    }

    @Override
    public Object getResponse() {
        return mapConfig;
    }
}
