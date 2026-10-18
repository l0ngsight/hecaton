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

package uk.org.hecaton.map.impl.nearcache;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nearcache.NearCacheRecord;
import uk.org.hecaton.internal.nearcache.NearCacheRecordStore;
import uk.org.hecaton.internal.nearcache.impl.DefaultNearCache;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.proxy.NearCachedMapProxyImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.internal.nearcache.NearCacheRecord.READ_PERMITTED;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.internal.util.RandomPicker.getInt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category(NightlyTest.class)
public class MapNearCacheRecordStateStressTest extends HecatonTestSupport {

    private static final int KEY_SPACE = 100;
    private static final int TEST_RUN_SECONDS = 60;
    private static final int GET_ALL_THREAD_COUNT = 3;
    private static final int GET_THREAD_COUNT = 7;
    private static final int PUT_LOCAL_THREAD_COUNT = 2;
    private static final int PUT_THREAD_COUNT = 2;
    private static final int CLEAR_THREAD_COUNT = 2;
    private static final int REMOVE_THREAD_COUNT = 2;

    private final TestHecatonInstanceFactory factory = new TestHecatonInstanceFactory();
    private final AtomicBoolean stop = new AtomicBoolean();

    private IMap<Integer, Integer> memberMap;
    private IMap<Integer, Integer> nearCachedMap;

    @Before
    public void setUp() {
        String mapName = randomMapName();

        Config config = getBaseConfig();

        MapConfig mapConfig = new MapConfig(mapName)
                .setNearCacheConfig(newNearCacheConfig(mapName));

        Config configWithNearCache = getBaseConfig()
                .addMapConfig(mapConfig);

        HecatonInstance member = factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);

        HecatonInstance nearCachedMember = factory.newHecatonInstance(configWithNearCache);

        memberMap = member.getMap(mapName);
        nearCachedMap = nearCachedMember.getMap(mapName);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void allRecordsAreInReadableStateInTheEnd() throws Exception {
        // initial population of IMap from member
        for (int i = 0; i < KEY_SPACE; i++) {
            memberMap.put(i, i);
        }

        List<Thread> threads = new ArrayList<>();
        // member
        for (int i = 0; i < PUT_THREAD_COUNT; i++) {
            Put put = new Put(memberMap);
            threads.add(put);
        }
        // member with Near Cache
        for (int i = 0; i < PUT_LOCAL_THREAD_COUNT; i++) {
            Put put = new Put(nearCachedMap);
            threads.add(put);
        }
        for (int i = 0; i < GET_ALL_THREAD_COUNT; i++) {
            GetAll getAll = new GetAll(nearCachedMap);
            threads.add(getAll);
        }
        for (int i = 0; i < GET_THREAD_COUNT; i++) {
            Get get = new Get(nearCachedMap);
            threads.add(get);
        }
        for (int i = 0; i < REMOVE_THREAD_COUNT; i++) {
            Remove remove = new Remove(nearCachedMap);
            threads.add(remove);
        }
        for (int i = 0; i < CLEAR_THREAD_COUNT; i++) {
            Clear clear = new Clear(nearCachedMap);
            threads.add(clear);
        }

        // start threads
        for (Thread thread : threads) {
            thread.start();
        }

        // stress for a while
        sleepSeconds(TEST_RUN_SECONDS);

        // stop threads
        stop.set(true);
        for (Thread thread : threads) {
            thread.join();
        }

        assertFinalRecordStateIsReadPermitted(nearCachedMap);
    }

    private NearCacheConfig newNearCacheConfig(String mapName) {
        return new NearCacheConfig(mapName)
                .setSerializeKeys(false)
                .setInvalidateOnChange(true);
    }

    private static void assertFinalRecordStateIsReadPermitted(IMap memberMap) {
        NearCachedMapProxyImpl proxy = (NearCachedMapProxyImpl) memberMap;
        DefaultNearCache nearCache = (DefaultNearCache) proxy.getNearCache().unwrap(DefaultNearCache.class);
        assertNearCacheRecordStates(nearCache, KEY_SPACE);
    }

    @SuppressWarnings("unchecked")
    public static void assertNearCacheRecordStates(DefaultNearCache nearCache, int keySpace) {
        NearCacheRecordStore nearCacheRecordStore = nearCache.getNearCacheRecordStore();
        if (nearCacheRecordStore.size() == 0) {
            return;
        }

        boolean recordFound = false;
        for (int i = 0; i < keySpace; i++) {
            NearCacheRecord record = nearCacheRecordStore.getRecord(i);
            if (record != null) {
                assertEquals(record.toString(), READ_PERMITTED, record.getReservationId());
                recordFound = true;
            }
        }
        // this is a fail-safe check, to be sure that we used the correct in-memory format for the key
        assertTrue("We should have found at least one NearCacheRecord", recordFound);
    }

    private class Put extends Thread {

        private final IMap<Integer, Integer> map;

        private Put(IMap<Integer, Integer> map) {
            this.map = map;
        }

        @Override
        public void run() {
            do {
                for (int i = 0; i < KEY_SPACE; i++) {
                    map.put(i, getInt(KEY_SPACE));
                }
                sleepAtLeastMillis(100);
            } while (!stop.get());
        }
    }

    private class Remove extends Thread {

        private final IMap<Integer, Integer> map;

        private Remove(IMap<Integer, Integer> map) {
            this.map = map;
        }

        @Override
        public void run() {
            do {
                for (int i = 0; i < KEY_SPACE; i++) {
                    map.remove(i);
                }
                sleepAtLeastMillis(100);
            } while (!stop.get());
        }
    }

    private class Clear extends Thread {

        private final IMap<Integer, Integer> map;

        private Clear(IMap<Integer, Integer> map) {
            this.map = map;
        }

        @Override
        public void run() {
            do {
                map.clear();
                sleepAtLeastMillis(5000);
            } while (!stop.get());
        }
    }

    private class GetAll extends Thread {

        private final IMap<Integer, Integer> map;

        private GetAll(IMap<Integer, Integer> map) {
            this.map = map;
        }

        @Override
        public void run() {
            HashSet<Integer> keys = new HashSet<>();
            for (int i = 0; i < KEY_SPACE; i++) {
                keys.add(i);
            }

            do {
                map.getAll(keys);
                sleepAtLeastMillis(2);
            } while (!stop.get());
        }
    }

    private class Get extends Thread {

        private final IMap<Integer, Integer> map;

        private Get(IMap<Integer, Integer> map) {
            this.map = map;
        }

        @Override
        public void run() {
            do {
                for (int i = 0; i < KEY_SPACE; i++) {
                    map.get(i);
                }
            } while (!stop.get());
        }
    }
}
