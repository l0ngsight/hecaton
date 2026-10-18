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

package uk.org.hecaton.map.impl.nearcache.invalidation;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nearcache.impl.invalidation.Invalidator;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MetaDataGenerator;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.nearcache.MapNearCacheManager;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberMapMetaDataGeneratorTest extends HecatonTestSupport {

    private static final String MAP_NAME = "MemberMapMetaDataGeneratorTest";

    @Test
    public void destroying_map_removes_related_metadata_when_near_cache_exists() {
        MapConfig mapConfig = getMapConfig(MAP_NAME);

        Config config = getConfig()
                .addMapConfig(mapConfig);

        HecatonInstance member = createHecatonInstance(config);

        IMap<Integer, Integer> map = member.getMap(MAP_NAME);
        map.put(1, 1);

        final MetaDataGenerator metaDataGenerator = getMetaDataGenerator(member);
        assertTrueEventually(() -> assertNotNull(metaDataGenerator.getSequenceGenerators().get(MAP_NAME)));

        map.destroy();

        assertNull(metaDataGenerator.getSequenceGenerators().get(MAP_NAME));
    }

    @Test
    public void destroying_map_removes_related_metadata_when_near_cache_not_exists() {
        Config config = getConfig();
        HecatonInstance member = createHecatonInstance(config);

        IMap<Integer, Integer> map = member.getMap(MAP_NAME);
        map.put(1, 1);

        final MetaDataGenerator metaDataGenerator = getMetaDataGenerator(member);
        assertTrueEventually(() -> assertNull(metaDataGenerator.getSequenceGenerators().get(MAP_NAME)));

        map.destroy();

        assertNull(metaDataGenerator.getSequenceGenerators().get(MAP_NAME));
    }

    protected Config getConfig() {
        return getBaseConfig();
    }

    protected MapConfig getMapConfig(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);

        return new MapConfig(mapName)
                .setNearCacheConfig(nearCacheConfig);
    }

    protected NearCacheConfig getNearCacheConfig(String mapName) {
        return new NearCacheConfig(mapName);
    }

    private static MetaDataGenerator getMetaDataGenerator(HecatonInstance member) {
        MapService mapService = getNodeEngineImpl(member).getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        return invalidator.getMetaDataGenerator();
    }
}
