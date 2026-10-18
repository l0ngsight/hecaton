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

package uk.org.hecaton.map.impl.mapstore.writebehind;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.internal.util.AbstractClockTest;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.mapstore.SingletonTestEntryStoreFactory;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
@Ignore("https://github.com/hazelcast/hazelcast-mono/pull/5908")
public class EntryStoreWriteBehindCustomClockTest extends AbstractClockTest {
    private IMap<String, String> map;

    @Before
    public void setup() {
        setClockOffset(Duration.ofHours(12).toMillis());
        startIsolatedNode(getConfig());
        map = isolatedNode.getMap(randomMapName());
    }

    @After
    public void teardown() {
        shutdownIsolatedNode();
        resetClock();
    }

    // If the Hecaton server clock differs from the system clock,
    // then when storing a value to the external system, the server time is converted to system time.
    @Test
    public void testPutWithExpirationTime() {
        map.put("key", "val", 1, TimeUnit.DAYS);
        long expectedExpirationTime = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1);
        long delta = 5000;
        assertTrueEventually(
            () -> invokeIsolatedInstanceMethod(
                SingletonTestEntryStoreFactory.class.getName(),
                "assertRecordStored",
                new Class[]{String.class, String.class, long.class, long.class},
                new Object[]{"key", "val", expectedExpirationTime, delta}
            )
        );
    }

    // If the Hecaton server clock differs from the system clock,
    // then when storing a value without expiration to the external system,
    // the correct time in the map store is NO_TIME.
    @Test
    public void testPutWithoutExpirationTime() {
        map.put("key", "val");
        assertTrueEventually(
            () -> invokeIsolatedInstanceMethod(
                SingletonTestEntryStoreFactory.class.getName(),
                "assertRecordStored",
                new Class[]{String.class, String.class},
                new Object[]{"key", "val"}
            )
        );
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig
            .setFactoryClassName(SingletonTestEntryStoreFactory.class.getName())
            .setWriteDelaySeconds(3)
            .setEnabled(true);
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        return config;
    }
}
