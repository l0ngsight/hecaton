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

package uk.org.hecaton.client.map.impl.nearcache;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.config.NearCachePreloaderConfig;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category(NightlyTest.class)
public class ClientMapNearCachePreloaderStressTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testDestroyAndCreateProxyWithNearCache() {
        Config config = getBaseConfig();

        ClientConfig clientConfig = new ClientConfig()
                .addNearCacheConfig(getNearCacheConfig("test"));

        factory.newHecatonInstance(config);
        final HecatonInstance client = factory.newHecatonClient(clientConfig);

        int createPutGetThreadCount = 2;
        int destroyThreadCount = 2;
        ExecutorService pool = newFixedThreadPool(createPutGetThreadCount + destroyThreadCount);

        final AtomicBoolean isRunning = new AtomicBoolean(true);
        final AtomicReference<Exception> exception = new AtomicReference<>();
        for (int i = 0; i < destroyThreadCount; i++) {
            pool.execute(() -> {
                while (isRunning.get()) {
                    for (DistributedObject distributedObject : client.getDistributedObjects()) {
                        distributedObject.destroy();
                    }
                }
            });
        }

        for (int i = 0; i < createPutGetThreadCount; i++) {
            pool.execute(() -> {
                try {
                    while (isRunning.get()) {
                        IMap<Object, Object> map = client.getMap("test");
                        map.put(1, 1);
                        map.get(1);
                    }
                } catch (Exception e) {
                    isRunning.set(false);
                    e.printStackTrace(System.out);
                    exception.set(e);
                }
            });
        }

        sleepSeconds(5);
        isRunning.set(false);
        pool.shutdown();
        assertNull(exception.get());
    }

    @SuppressWarnings("SameParameterValue")
    private NearCacheConfig getNearCacheConfig(String name) {
        NearCachePreloaderConfig preloaderConfig = new NearCachePreloaderConfig()
                .setStoreIntervalSeconds(1)
                .setEnabled(true);

        return new NearCacheConfig()
                .setName(name)
                .setPreloaderConfig(preloaderConfig);
    }
}
