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

package uk.org.hecaton.client.map.impl.nearcache.invalidation;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.spi.ClientContext;
import uk.org.hecaton.client.impl.spi.ClientProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher;
import uk.org.hecaton.internal.nearcache.impl.invalidation.Invalidator;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MetaDataContainer;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MetaDataGenerator;
import uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingHandler;
import uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.nearcache.MapNearCacheManager;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.internal.util.RandomPicker.getInt;
import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapInvalidationMetaDataFetcherTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void fetches_sequence_and_uuid() {
        String mapName = "test";
        int partition = 1;
        long givenSequence = getInt(1, Integer.MAX_VALUE);
        UUID givenUuid = UuidUtil.newUnsecureUUID();

        RepairingTask repairingTask = getRepairingTask(mapName, partition, givenSequence, givenUuid);
        InvalidationMetaDataFetcher invalidationMetaDataFetcher = repairingTask.getInvalidationMetaDataFetcher();
        ConcurrentMap<String, RepairingHandler> handlers = repairingTask.getHandlers();
        invalidationMetaDataFetcher.fetchMetadata(handlers);

        RepairingHandler repairingHandler = handlers.get(mapName);
        MetaDataContainer metaDataContainer = repairingHandler.getMetaDataContainer(partition);
        UUID foundUuid = metaDataContainer.getUuid();
        long foundSequence = metaDataContainer.getSequence();

        assertEquals(givenSequence, foundSequence);
        assertEquals(givenUuid, foundUuid);
    }

    private RepairingTask getRepairingTask(String mapName, int partition, long givenSequence, UUID givenUuid) {
        Config config = getBaseConfig();
        HecatonInstance member = factory.newHecatonInstance(config);
        distortRandomPartitionSequence(mapName, partition, givenSequence, member);
        distortRandomPartitionUuid(partition, givenUuid, member);

        ClientConfig clientConfig = new ClientConfig().addNearCacheConfig(new NearCacheConfig(mapName));
        HecatonInstance client = factory.newHecatonClient(clientConfig);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        ClientContext clientContext = ((ClientProxy) clientMap).getContext();
        return clientContext.getRepairingTask(SERVICE_NAME);
    }

    private void distortRandomPartitionSequence(String mapName, int partition, long sequence, HecatonInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        MetaDataGenerator metaDataGenerator = invalidator.getMetaDataGenerator();
        metaDataGenerator.setCurrentSequence(mapName, partition, sequence);
    }

    private void distortRandomPartitionUuid(int partition, UUID uuid, HecatonInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        MetaDataGenerator metaDataGenerator = invalidator.getMetaDataGenerator();
        metaDataGenerator.setUuid(partition, uuid);
    }
}
