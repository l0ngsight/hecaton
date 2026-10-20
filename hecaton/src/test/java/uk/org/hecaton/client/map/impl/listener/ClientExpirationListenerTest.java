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

package uk.org.hecaton.client.map.impl.listener;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.listener.EntryEvictedListener;
import uk.org.hecaton.map.listener.EntryExpiredListener;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExpirationListenerTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();
    private HecatonInstance client;
    private IMap map;

    @Before
    public void setup() {
        hecatonFactory.newHecatonInstance();
        client = hecatonFactory.newHecatonClient();
        map = client.getMap(randomName());
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testExpirationListener_notified_afterExpirationOfEntries() {
        int numberOfPutOperations = 1000;
        CountDownLatch expirationEventArrivalCount = new CountDownLatch(numberOfPutOperations);

        map.addEntryListener(new ExpirationListener(expirationEventArrivalCount), true);

        for (int i = 0; i < numberOfPutOperations; i++) {
            map.put(i, i, 100, TimeUnit.MILLISECONDS);
        }

        // wait expiration of entries.
        sleepAtLeastMillis(200);

        // trigger immediate fire of expiration events by touching them.
        for (int i = 0; i < numberOfPutOperations; i++) {
            map.get(i);
        }

        assertOpenEventually(expirationEventArrivalCount);
    }

    private static class ExpirationListener implements EntryExpiredListener {

        private final CountDownLatch expirationEventCount;

        ExpirationListener(CountDownLatch expirationEventCount) {
            this.expirationEventCount = expirationEventCount;
        }

        @Override
        public void entryExpired(EntryEvent event) {
            expirationEventCount.countDown();
        }
    }

    /**
     * Should notify only expiration listener.
     */
    @Test
    public void expiration_and_eviction_listener_are_not_both_notified_after_expiration() {
        int numberOfPutOperations = 1000;

        ExpirationAndEvictionListener listener = new ExpirationAndEvictionListener();
        map.addEntryListener(listener, true);

        for (int i = 0; i < numberOfPutOperations; i++) {
            map.put(i, i, 100, TimeUnit.MILLISECONDS);
        }

        // wait expiration of entries.
        sleepAtLeastMillis(200);

        // trigger immediate fire of expiration events by touching them.
        for (int i = 0; i < numberOfPutOperations; i++) {
            map.get(i);
        }

        assertTrueEventually(() -> assertEquals(numberOfPutOperations,
                listener.expirationEventArrivalCount.get()));
        assertTrueAllTheTime(() -> assertEquals(0,
                listener.evictionEventArrivalCount.get()), 3);
    }


    private static class ExpirationAndEvictionListener implements EntryExpiredListener, EntryEvictedListener {

        private final AtomicInteger expirationEventArrivalCount = new AtomicInteger();
        private final AtomicInteger evictionEventArrivalCount = new AtomicInteger();

        ExpirationAndEvictionListener() {
        }

        @Override
        public void entryExpired(EntryEvent event) {
            expirationEventArrivalCount.incrementAndGet();
        }

        @Override
        public void entryEvicted(EntryEvent event) {
            evictionEventArrivalCount.incrementAndGet();
        }
    }

}
