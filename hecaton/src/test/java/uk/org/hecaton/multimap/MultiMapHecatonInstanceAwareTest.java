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

package uk.org.hecaton.multimap;

import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.EntryListener;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.map.MapEvent;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiMapHecatonInstanceAwareTest extends HecatonTestSupport {
    private CompletableFuture<HecatonInstance> hecatonInstanceFuture;

    @Before
    public void setUp() {
        hecatonInstanceFuture = new CompletableFuture<>();
    }

    @Test
    public void testInjected() {
        createHecatonInstance().getMultiMap(randomMapName()).addEntryListener(new MyEntryListener<>(), false);

        assertTrueEventually(() -> assertNotNull(hecatonInstanceFuture.get()));
    }

    private class MyEntryListener<K, V> implements EntryListener<K, V>, HecatonInstanceAware {
        @Override
        public void entryAdded(EntryEvent<K, V> event) {
        }

        @Override
        public void entryUpdated(EntryEvent<K, V> event) {
        }

        @Override
        public void entryRemoved(EntryEvent<K, V> event) {
        }

        @Override
        public void entryEvicted(EntryEvent<K, V> event) {
        }

        @Override
        public void entryExpired(EntryEvent<K, V> event) {
        }

        @Override
        public void mapCleared(MapEvent event) {
        }

        @Override
        public void mapEvicted(MapEvent event) {
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            if (hecatonInstance != null) {
                hecatonInstanceFuture.complete(hecatonInstance);
            }
        }
    }
}
