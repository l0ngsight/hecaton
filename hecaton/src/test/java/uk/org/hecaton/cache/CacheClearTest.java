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

package uk.org.hecaton.cache;

import uk.org.hecaton.cache.impl.CacheEventListener;
import uk.org.hecaton.cache.impl.ICacheRecordStore;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceProxy;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nearcache.impl.invalidation.Invalidation;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheClearTest extends CacheTestSupport {

    private static final int INSTANCE_COUNT = 2;

    private final TestHecatonInstanceFactory factory = getInstanceFactory(INSTANCE_COUNT);
    private HecatonInstance[] hecatonInstances;
    private HecatonInstance hecatonInstance;

    protected TestHecatonInstanceFactory getInstanceFactory(int instanceCount) {
        return createHecatonInstanceFactory(instanceCount);
    }

    @Override
    protected void onSetup() {
        hecatonInstances = new HecatonInstance[INSTANCE_COUNT];
        for (int i = 0; i < hecatonInstances.length; i++) {
            hecatonInstances[i] = factory.newHecatonInstance(createConfig());
        }
        warmUpPartitions(hecatonInstances);
        waitAllForSafeState(hecatonInstances);
        hecatonInstance = hecatonInstances[0];
    }

    @Override
    protected void onTearDown() {
        factory.shutdownAll();
        hecatonInstances = null;
        hecatonInstance = null;
    }

    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        CacheConfig cacheConfig = super.createCacheConfig();
        cacheConfig.setBackupCount(INSTANCE_COUNT - 1);
        return cacheConfig;
    }

    @Override
    protected HecatonInstance getHecatonInstance() {
        return hecatonInstance;
    }

    protected Map<String, String> createAndFillEntries() {
        final int ENTRY_COUNT_PER_PARTITION = 3;
        Node node = getNode(hecatonInstance);
        int partitionCount = node.getPartitionService().getPartitionCount();
        Map<String, String> entries = new HashMap<>(partitionCount * ENTRY_COUNT_PER_PARTITION);

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            for (int i = 0; i < ENTRY_COUNT_PER_PARTITION; i++) {
                String key = generateKeyForPartition(hecatonInstance, partitionId);
                String value = generateRandomString(16);
                entries.put(key, value);
            }
        }

        return entries;
    }

    @Test
    public void testClear() {
        ICache<String, String> cache = createCache();
        String cacheName = cache.getName();
        Map<String, String> entries = createAndFillEntries();

        for (Map.Entry<String, String> entry : entries.entrySet()) {
            cache.put(entry.getKey(), entry.getValue());
        }

        // Verify that put works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entries.get(key);
            String actualValue = cache.get(key);
            assertEquals(expectedValue, actualValue);
        }

        Node node = getNode(hecatonInstance);
        InternalPartitionService partitionService = node.getPartitionService();
        SerializationService serializationService = node.getSerializationService();

        // Verify that backup of put works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entries.get(key);
            Data keyData = serializationService.toData(key);
            int keyPartitionId = partitionService.getPartitionId(keyData);
            for (int i = 0; i < INSTANCE_COUNT; i++) {
                Node n = getNode(hecatonInstances[i]);
                ICacheService cacheService = n.getNodeEngine().getService(ICacheService.SERVICE_NAME);
                ICacheRecordStore recordStore = cacheService.getRecordStore("/hz/" + cacheName, keyPartitionId);
                assertNotNull(recordStore);
                String actualValue = serializationService.toObject(recordStore.get(keyData, null));
                assertEquals(expectedValue, actualValue);
            }
        }

        cache.clear();

        // Verify that clear works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String actualValue = cache.get(key);
            assertNull(actualValue);
        }

        // Verify that backup of clear works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            Data keyData = serializationService.toData(key);
            int keyPartitionId = partitionService.getPartitionId(keyData);
            for (int i = 0; i < INSTANCE_COUNT; i++) {
                Node n = getNode(hecatonInstances[i]);
                ICacheService cacheService = n.getNodeEngine().getService(ICacheService.SERVICE_NAME);
                ICacheRecordStore recordStore = cacheService.getRecordStore("/hz/" + cacheName, keyPartitionId);
                assertNotNull(recordStore);
                String actualValue = serializationService.toObject(recordStore.get(keyData, null));
                assertNull(actualValue);
            }
        }
    }

    @Test
    public void testInvalidationListenerCallCount() {
        final ICache<String, String> cache = createCache();
        Map<String, String> entries = createAndFillEntries();

        for (Map.Entry<String, String> entry : entries.entrySet()) {
            cache.put(entry.getKey(), entry.getValue());
        }

        // Verify that put works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entries.get(key);
            String actualValue = cache.get(key);
            assertEquals(expectedValue, actualValue);
        }

        final AtomicInteger counter = new AtomicInteger();

        final CacheConfig config = cache.getConfiguration(CacheConfig.class);

        registerInvalidationListener(eventObject -> {
            if (eventObject instanceof Invalidation event) {
                if (null == event.getKey() && config.getNameWithPrefix().equals(event.getName())) {
                    counter.incrementAndGet();
                }
            }
        }, config.getNameWithPrefix());

        cache.clear();

        // Make sure that one event is received
        assertTrueEventually(() -> assertEquals(1, counter.get()), 5);

        // Make sure that the callback is not called for a while
        assertTrueAllTheTime(() -> assertTrue(counter.get() <= 1), 3);

    }

    private void registerInvalidationListener(CacheEventListener cacheEventListener, String name) {
        HecatonInstanceProxy hzInstance = (HecatonInstanceProxy) this.hecatonInstance;
        hzInstance.getOriginal().node.getNodeEngine().getEventService()
                .registerListener(ICacheService.SERVICE_NAME, name, cacheEventListener);
    }
}
