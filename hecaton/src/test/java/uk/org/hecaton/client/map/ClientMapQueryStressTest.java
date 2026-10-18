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

package uk.org.hecaton.client.map;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({SlowTest.class})
public class ClientMapQueryStressTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();
    private final AtomicBoolean stop = new AtomicBoolean();
    private final AtomicInteger mapSizeHolder = new AtomicInteger();
    private final AtomicInteger keySetSizeHolder = new AtomicInteger();
    private final List<Thread> threads = new ArrayList<>();

    private IMap map;

    @Before
    public void setUp() {
        for (int i = 0; i < 3; i++) {
            factory.newHecatonInstance();
        }

        HecatonInstance client = factory.newHecatonClient();

        map = client.getMap("test");
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @org.junit.Test
    public void map_size__equals__map_keySet_size() throws Exception {
        for (int i = 0; i < 5; i++) {
            threads.add(new PutAllThread());
        }

        for (int i = 0; i < 5; i++) {
            threads.add(new KeySetThread());
        }

        // tests main assertion `map#keySet#size() == map#size()`
        threads.add(new SizeTesterThread());

        startThreads();

        sleepSeconds(10);

        stopThreadsAndWaitThemToDie();

        assertEquals("mapSize=" + mapSizeHolder.get() + ", keySetSize=" + keySetSizeHolder.get(),
                mapSizeHolder.get(), keySetSizeHolder.get());

    }

    protected void stopThreadsAndWaitThemToDie() throws InterruptedException {
        stop.set(true);

        for (Thread thread : threads) {
            thread.join();
        }
    }

    protected void startThreads() {
        for (Thread thread : threads) {
            thread.start();
        }
    }

    protected Map getMap() {
        Map batch = new HashMap();
        for (int i = 0; i < 30; i++) {
            batch.put(i, i);
        }
        return batch;
    }

    private class PutAllThread extends Thread {

        @Override
        public void run() {
            while (!stop.get()) {
                Map batch = getMap();
                map.putAll(batch);
            }
        }
    }

    private class KeySetThread extends Thread {

        @Override
        public void run() {
            while (!stop.get()) {
                map.keySet();
            }
        }
    }

    private class SizeTesterThread extends Thread {

        @Override
        public void run() {
            while (!stop.get()) {
                Map batch = getMap();
                map.putAll(batch);

                int mapSize = map.size();
                int keySetSize = map.keySet().size();

                if (mapSize != keySetSize) {
                    mapSizeHolder.set(mapSize);
                    keySetSizeHolder.set(keySetSize);
                    stop.set(true);
                    break;
                }
            }
        }
    }
}
