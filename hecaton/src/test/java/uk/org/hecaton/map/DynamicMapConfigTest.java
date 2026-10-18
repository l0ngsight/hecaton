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

package uk.org.hecaton.map;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.management.operation.UpdateMapConfigOperation;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.listener.EntryEvictedListener;
import uk.org.hecaton.map.listener.EntryExpiredListener;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.ChangeLoggingRule;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Map configuration can be updated dynamically at runtime by using management center ui.
 * This test verifies that the changes will be reflected to corresponding IMap at runtime.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicMapConfigTest extends HecatonTestSupport {

    private final ILogger logger = Logger.getLogger(getClass());

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-trace-dynamic-map-config-update.xml");

    @Test
    public void testMapConfigUpdate_reflectedToRecordStore() throws InterruptedException {
        String mapName = randomMapName();
        CountDownLatch expiredLatch = new CountDownLatch(1);
        CountDownLatch evictedLatch = new CountDownLatch(1);
        final AtomicReference<String> failureMessage = new AtomicReference<>();

        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        HecatonInstance node = createHecatonInstance(config);

        IMap<Integer, Integer> map = node.getMap(mapName);

        // add listeners
        map.addEntryListener((EntryExpiredListener<Integer, Integer>) event -> {
            logger.info("Entry expired: " + event);
            expiredLatch.countDown();
        }, 1, false);
        map.addEntryListener((EntryEvictedListener<Integer, Integer>) event -> {
            logger.info("Entry evicted: " + event);
            // ensure the correct key was evicted
            if (event.getKey() != 2) {
                failureMessage.set(String.format("Expected eviction of key '2', but received key '%d' instead",
                        event.getKey()));
            }
            evictedLatch.countDown();
        }, false);

        // trigger recordStore creation
        map.put(1, 1);

        updateMapConfig(mapName, node);
        // trigger recordStore expiry system, only added/updated
        // entries after config update will be affected.
        map.put(1, 1);

        assertTrue("Entry didn't expire", expiredLatch.await(60, TimeUnit.SECONDS));

        // test map size to ensure the expired entry was removed
        assertEquals(0, map.size());

        // test eviction with infinite ttl and max-idle
        map.put(2, 2, 0, TimeUnit.SECONDS, 0, TimeUnit.SECONDS);
        map.put(3, 3);

        assertEquals(1, map.size());

        assertTrue("Entry didn't evict", evictedLatch.await(60, TimeUnit.SECONDS));

        // check if eviction resulted in a failure message
        if (failureMessage.get() != null) {
            fail("Eviction failed: " + failureMessage.get());
        }
    }

    private void updateMapConfig(String mapName, HecatonInstance node) {
        MapConfig mapConfig = createMapConfig();
        Operation updateMapConfigOperation = new UpdateMapConfigOperation(
                mapName,
                mapConfig.getTimeToLiveSeconds(),
                mapConfig.getMaxIdleSeconds(),
                mapConfig.getEvictionConfig().getSize(),
                mapConfig.getEvictionConfig().getMaxSizePolicy().getId(),
                mapConfig.isReadBackupData(),
                mapConfig.getEvictionConfig().getEvictionPolicy().getId(),
                true,
                null);
        executeOperation(node, updateMapConfigOperation);
    }

    private MapConfig createMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setTimeToLiveSeconds(1);
        mapConfig.setMaxIdleSeconds(22);
        mapConfig.setReadBackupData(false);
        mapConfig.setBackupCount(3);
        mapConfig.setAsyncBackupCount(2);
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);
        evictionConfig.setSize(1).setMaxSizePolicy(MaxSizePolicy.PER_NODE);
        return mapConfig;
    }

    private void executeOperation(HecatonInstance node, Operation op) {
        OperationServiceImpl operationService = getOperationService(node);
        Address address = getAddress(node);
        operationService.invokeOnTarget(MapService.SERVICE_NAME, op, address).join();
    }
}
