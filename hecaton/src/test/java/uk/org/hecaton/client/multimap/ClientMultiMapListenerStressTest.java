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

package uk.org.hecaton.client.multimap;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.multimap.MultiMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.test.HecatonTestSupport.assertJoinable;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static uk.org.hecaton.test.HecatonTestSupport.randomString;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category(NightlyTest.class)
public class ClientMultiMapListenerStressTest {

    private static final int MAX_SECONDS = 60 * 10;
    private static final int NUMBER_OF_CLIENTS = 8;
    private static final int THREADS_PER_CLIENT = 4;
    private static final String MAP_NAME = randomString();

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();
    private HecatonInstance server;

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void setup() {
        Config cfg = new Config();
        cfg.setProperty("hecaton.event.queue.capacity", "5000000");
        server = hecatonFactory.newHecatonInstance(cfg);
    }

    @Test
    public void listenerAddStressTest() {
        final PutItemsThread[] putThreads = new PutItemsThread[NUMBER_OF_CLIENTS * THREADS_PER_CLIENT];

        int idx = 0;
        for (int i = 0; i < NUMBER_OF_CLIENTS; i++) {
            HecatonInstance client = hecatonFactory.newHecatonClient();
            for (int j = 0; j < THREADS_PER_CLIENT; j++) {
                PutItemsThread t = new PutItemsThread(client);
                putThreads[idx++] = t;
            }
        }

        for (int i = 0; i < putThreads.length; i++) {
            putThreads[i].start();
        }
        MultiMap multiMap = server.getMultiMap(MAP_NAME);


        assertJoinable(MAX_SECONDS, putThreads);

        final int expectedSize = PutItemsThread.MAX_ITEMS * putThreads.length;
        assertEquals(expectedSize, multiMap.size());
        assertReceivedEventsSize(expectedSize, putThreads);
    }

    private void assertReceivedEventsSize(final int expectedSize, final PutItemsThread[] putThreads) {
        for (int i = 0; i < putThreads.length; i++) {
            putThreads[i].assertResult(expectedSize);
        }
    }

    public class PutItemsThread extends Thread {
        public static final int MAX_ITEMS = 100;

        public final MyEntryListener listener = new MyEntryListener();
        public HecatonInstance client;
        public MultiMap mm;
        public String id;

        public PutItemsThread(HecatonInstance client) {
            this.id = randomString();
            this.client = client;
            this.mm = client.getMultiMap(MAP_NAME);
            mm.addEntryListener(listener, true);
        }

        @Override
        public void run() {
            for (int i = 0; i < MAX_ITEMS; i++) {
                mm.put(id + i, id + i);
            }
        }

        public void assertResult(final int target) {
            assertTrueEventually(() -> assertEquals(target, listener.add.get()));
        }
    }

    static class MyEntryListener extends EntryAdapter {
        public AtomicInteger add = new AtomicInteger();

        public void entryAdded(EntryEvent event) {
            add.incrementAndGet();
        }
    }
}
