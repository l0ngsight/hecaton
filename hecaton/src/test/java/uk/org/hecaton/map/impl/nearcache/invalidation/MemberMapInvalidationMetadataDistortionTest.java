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
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nearcache.impl.invalidation.Invalidator;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MetaDataGenerator;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.nearcache.MapNearCacheManager;
import uk.org.hecaton.map.impl.nearcache.NearCacheTestSupport;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static uk.org.hecaton.internal.util.RandomPicker.getInt;
import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category(NightlyTest.class)
public class MemberMapInvalidationMetadataDistortionTest extends NearCacheTestSupport {

    private static final int MAP_SIZE = 100000;
    private static final String MAP_NAME = "MemberMapInvalidationMetadataDistortionTest";

    private final TestHecatonInstanceFactory factory = new TestHecatonInstanceFactory();
    private final AtomicBoolean stopTest = new AtomicBoolean();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void lostInvalidation() {
        // members are created
        MapConfig mapConfig = createMapConfig(MAP_NAME);
        Config config = createConfig().addMapConfig(mapConfig);
        final HecatonInstance member = factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);

        // map is populated form member
        final IMap<Integer, Integer> memberMap = member.getMap(MAP_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            memberMap.put(i, i);
        }

        // a new member comes with Near Cache configured
        MapConfig nearCachedMapConfig = createMapConfig(MAP_NAME).setNearCacheConfig(createNearCacheConfig(MAP_NAME));
        Config nearCachedConfig = createConfig().addMapConfig(nearCachedMapConfig);
        HecatonInstance nearCachedMember = factory.newHecatonInstance(nearCachedConfig);
        final IMap<Integer, Integer> nearCachedMap = nearCachedMember.getMap(MAP_NAME);

        Thread populateNearCache = new Thread(() -> {
            while (!stopTest.get()) {
                for (int i = 0; i < MAP_SIZE; i++) {
                    nearCachedMap.get(i);
                }
            }
        });

        Thread distortSequence = new Thread(() -> {
            while (!stopTest.get()) {
                distortRandomPartitionSequence(MAP_NAME, member);
                sleepSeconds(1);
            }
        });

        Thread distortUuid = new Thread(() -> {
            while (!stopTest.get()) {
                distortRandomPartitionUuid(member);
                sleepSeconds(3);
            }
        });

        Thread put = new Thread(() -> {
            // change some data
            while (!stopTest.get()) {
                int key = getInt(MAP_SIZE);
                int value = getInt(Integer.MAX_VALUE);
                memberMap.put(key, value);
                sleepAtLeastMillis(10);
            }
        });

        // start threads
        put.start();
        populateNearCache.start();
        distortSequence.start();
        distortUuid.start();

        sleepSeconds(60);

        // stop threads
        stopTest.set(true);
        assertJoinable(distortUuid, distortSequence, populateNearCache, put);

        assertTrueEventually(() -> {
            for (int i = 0; i < MAP_SIZE; i++) {
                Integer valueSeenFromMember = memberMap.get(i);
                Integer valueSeenFromNearCachedSide = nearCachedMap.get(i);

                assertEquals(valueSeenFromMember, valueSeenFromNearCachedSide);
            }
        });
    }

    private Config createConfig() {
        return smallInstanceConfig()
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "0")
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true")
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), "10000")
                .setProperty(PARTITION_COUNT.getName(), "271");
    }

    private MapConfig createMapConfig(String mapName) {
        return new MapConfig(mapName)
                .setBackupCount(0);
    }

    private NearCacheConfig createNearCacheConfig(String mapName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(Integer.MAX_VALUE)
                .setEvictionPolicy(EvictionPolicy.NONE);

        return newNearCacheConfig()
                .setName(mapName)
                .setInvalidateOnChange(true)
                .setEvictionConfig(evictionConfig);
    }

    private void distortRandomPartitionSequence(String mapName, HecatonInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        MetaDataGenerator metaDataGenerator = invalidator.getMetaDataGenerator();
        InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        metaDataGenerator.setCurrentSequence(mapName, getInt(partitionCount), getInt(MAX_VALUE));
    }

    private void distortRandomPartitionUuid(HecatonInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        int partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
        int partitionId = getInt(partitionCount);
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        MetaDataGenerator metaDataGenerator = invalidator.getMetaDataGenerator();

        metaDataGenerator.setUuid(partitionId, UuidUtil.newUnsecureUUID());
    }
}
