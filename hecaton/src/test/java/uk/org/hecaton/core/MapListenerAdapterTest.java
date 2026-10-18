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

package uk.org.hecaton.core;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapListenerAdapter;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapListenerAdapterTest extends HecatonTestSupport {

    @Test
    public void testMapListenerAdapter_whenEntryExpired() {
        String mapName = randomMapName();
        Config cfg = new Config();

        TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory(1);
        HecatonInstance instance = instanceFactory.newHecatonInstance(cfg);

        IMap<Integer, Integer> map = instance.getMap(mapName);

        final CountDownLatch expirationLatch = new CountDownLatch(1);
        map.addEntryListener(new MapListenerAdapter<Integer, Integer>() {
            @Override
            public void onEntryEvent(EntryEvent<Integer, Integer> event) {
                expirationLatch.countDown();
            }
        }, false);

        map.put(1, 1, 100, TimeUnit.MILLISECONDS);
        sleepSeconds(1);

        // trigger immediate expiration.
        map.get(1);

        assertOpenEventually(expirationLatch);
    }

}
