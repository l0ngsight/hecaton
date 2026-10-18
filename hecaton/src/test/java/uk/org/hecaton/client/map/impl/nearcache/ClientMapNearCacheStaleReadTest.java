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
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static java.lang.Integer.parseInt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests that the Near Cache doesn't lose invalidations.
 * <p>
 * Issue: https://github.com/hazelcast/hazelcast/issues/4671
 * <p>
 * Thanks Lukas Blunschi for the original test (https://github.com/lukasblu).
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class ClientMapNearCacheStaleReadTest extends HecatonTestSupport {

    private static final int NUM_GETTERS = 7;
    private static final int MAX_RUNTIME = 30;
    private static final String KEY = "key123";

    private static final ILogger LOGGER = Logger.getLogger(ClientMapNearCacheStaleReadTest.class);

    private AtomicInteger valuePut = new AtomicInteger();
    private AtomicBoolean stop = new AtomicBoolean();
    private AtomicInteger assertionViolationCount = new AtomicInteger();
    private AtomicBoolean failed = new AtomicBoolean();

    private HecatonInstance member;
    private HecatonInstance client;
    private IMap<String, String> map;

    @Before
    public void setUp() {
        String mapName = randomMapName();
        TestHecatonFactory factory = new TestHecatonFactory();

        Config config = getBaseConfig()
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), "2");

        ClientConfig clientConfig = getClientConfig(mapName)
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "0");

        member = factory.newHecatonInstance(config);
        client = factory.newHecatonClient(clientConfig);
        map = client.getMap(mapName);
    }

    @After
    public void tearDown() {
        client.shutdown();
        member.shutdown();
    }

    @Test
    public void testNoLostInvalidationsEventually() {
        testNoLostInvalidationsStrict(false);
    }

    @Test
    public void testNoLostInvalidationsStrict() {
        testNoLostInvalidationsStrict(true);
    }

    private void testNoLostInvalidationsStrict(boolean strict) {
        // run test
        runTestInternal();

        if (!strict) {
            // test eventually consistent
            sleepSeconds(2);
        }
        int valuePutLast = valuePut.get();
        String valueMapStr = map.get(KEY);
        int valueMap = parseInt(valueMapStr);

        // fail if not eventually consistent
        String msg = null;
        if (valueMap < valuePutLast) {
            msg = "Near Cache did *not* become consistent. (valueMap = " + valueMap + ", valuePut = " + valuePutLast + ").";

            // flush Near Cache and re-fetch value
            flushClientNearCache(map);
            String valueMap2Str = map.get(KEY);
            int valueMap2 = parseInt(valueMap2Str);

            // test again
            if (valueMap2 < valuePutLast) {
                msg += " Unexpected inconsistency! (valueMap2 = " + valueMap2 + ", valuePut = " + valuePutLast + ").";
            } else {
                msg += " Flushing the Near Cache cleared the inconsistency. (valueMap2 = " + valueMap2
                        + ", valuePut = " + valuePutLast + ").";
            }
        }

        // stop instance
        client.getLifecycleService().terminate();

        // fail after stopping instance
        if (msg != null) {
            LOGGER.warning(msg);
            fail(msg);
        }

        // fail if strict is required and assertion was violated
        if (strict && assertionViolationCount.get() > 0) {
            msg = "Assertion violated " + assertionViolationCount.get() + " times.";
            LOGGER.warning(msg);
            fail(msg);
        }
    }

    protected ClientConfig getClientConfig(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);

        return new ClientConfig()
                .addNearCacheConfig(nearCacheConfig);
    }

    protected NearCacheConfig getNearCacheConfig(String mapName) {
        return new NearCacheConfig(mapName)
                .setInMemoryFormat(InMemoryFormat.BINARY);
    }

    /**
     * Flush Near Cache from client map with Near Cache.
     * <p>
     * Warning: this uses Hecaton internals which might change from one version to the other.
     */
    private void flushClientNearCache(IMap map) {
        if (!(map instanceof NearCachedClientMapProxy)) {
            return;
        }

        NearCachedClientMapProxy clientMapProxy = (NearCachedClientMapProxy) map;
        clientMapProxy.getNearCache().clear();
    }

    private void runTestInternal() {
        // start 1 putter thread (put0)
        Thread threadPut = new Thread(new PutRunnable(), "put0");
        threadPut.start();

        // wait for putter thread to start before starting getter threads
        sleepMillis(300);

        // start numGetters getter threads (get0-numGetters)
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < NUM_GETTERS; i++) {
            Thread thread = new Thread(new GetRunnable(), "get" + i);
            threads.add(thread);
        }
        for (Thread thread : threads) {
            thread.start();
        }

        // stop after maxRuntime seconds
        int i = 0;
        while (!stop.get() && i++ < MAX_RUNTIME) {
            sleepMillis(1000);
        }
        if (!stop.get()) {
            LOGGER.info("Problem did not occur within " + MAX_RUNTIME + "s.");
        }
        stop.set(true);
        assertJoinable(threadPut);
        for (Thread thread : threads) {
            assertJoinable(thread);
        }
    }

    private class PutRunnable implements Runnable {

        @Override
        public void run() {
            LOGGER.info(Thread.currentThread().getName() + " started.");
            int i = 0;
            while (!stop.get()) {
                i++;

                // put new value and update last state
                // note: the value in the map/Near Cache is *always* larger or equal to valuePut
                // assertion: valueMap >= valuePut
                map.put(KEY, String.valueOf(i));
                valuePut.set(i);

                // check if we see our last update
                String valueMapStr = map.get(KEY);
                if (valueMapStr == null) {
                    continue;
                }
                int valueMap = parseInt(valueMapStr);
                if (valueMap != i) {
                    assertionViolationCount.incrementAndGet();
                    LOGGER.warning("Assertion violated! (valueMap = " + valueMap + ", i = " + i + ")");

                    // sleep to ensure Near Cache invalidation is really lost
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOGGER.warning("Interrupted: " + e.getMessage());
                    }

                    // test again and stop if really lost
                    valueMapStr = map.get(KEY);
                    valueMap = parseInt(valueMapStr);
                    if (valueMap != i) {
                        LOGGER.warning("Near Cache invalidation lost! (valueMap = " + valueMap + ", i = " + i + ")");
                        failed.set(true);
                        stop.set(true);
                    }
                }
            }
            LOGGER.info(Thread.currentThread().getName() + " performed " + i + " operations.");
        }
    }

    private class GetRunnable implements Runnable {

        @Override
        public void run() {
            LOGGER.info(Thread.currentThread().getName() + " started.");
            int n = 0;
            while (!stop.get()) {
                n++;

                // blindly get the value (to trigger the issue) and parse the value (to get some CPU load)
                String valueMapStr = map.get(KEY);
                int i = parseInt(valueMapStr);
                assertEquals("" + i, valueMapStr);
            }
            LOGGER.info(Thread.currentThread().getName() + " performed " + n + " operations.");
        }
    }
}
