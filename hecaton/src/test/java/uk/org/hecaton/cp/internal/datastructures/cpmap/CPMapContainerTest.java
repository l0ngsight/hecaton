/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.cp.internal.datastructures.cpmap;

import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPMapContainerTest {

    private final InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
    private final RaftGroupId groupId = new RaftGroupId("group", 0, 1);

    @Test
    public void sizeIsTrackedAcrossOperations() {
        CPMapContainer map = new CPMapContainer(groupId, "m", null, Long.MAX_VALUE);
        Data k = data("k");
        map.put(k, data("value"), 1);
        long one = map.sizeBytes();
        assertEquals(k.totalSize() + data("value").totalSize(), one);

        map.put(k, data("longer value"), 2);
        assertEquals(k.totalSize() + data("longer value").totalSize(), map.sizeBytes());

        assertTrue(map.compareAndSet(k, data("longer value"), data("value"), 3));
        assertEquals(one, map.sizeBytes());

        map.remove(k);
        assertEquals(0, map.sizeBytes());
        assertEquals(0, map.size());
    }

    @Test
    public void putBeyondLimitLeavesMapUnchanged() {
        Data k = data("k");
        Data v = data("v");
        long limit = k.totalSize() + v.totalSize();
        CPMapContainer map = new CPMapContainer(groupId, "m", null, limit);
        map.put(k, v, 1);

        assertThrows(IllegalStateException.class, () -> map.put(data("k2"), v, 2));
        assertThrows(IllegalStateException.class, () -> map.compareAndSet(k, v, data("much longer"), 3));
        assertEquals(1, map.size());
        assertEquals(v, map.get(k));
        assertEquals(limit, map.sizeBytes());
    }

    @Test
    public void purgeRemovesOnlyOlderEntries() {
        CPMapContainer map = new CPMapContainer(groupId, "m", null, Long.MAX_VALUE);
        map.put(data("a"), data("1"), 100);
        map.put(data("b"), data("2"), 200);
        map.put(data("c"), data("3"), 300);
        map.put(data("a"), data("1"), 400);

        CPMapPurgeResponseImpl response = map.purge(250);

        assertEquals(1, response.purgedCount());
        assertEquals(300, response.oldestRemainingEntryTimestamp());
        assertNull(map.get(data("b")));
        assertEquals(2, map.size());
    }

    @Test
    public void snapshotIsACopyAndRoundTrips() {
        CPMapContainer map = new CPMapContainer(groupId, "m", null, Long.MAX_VALUE);
        map.put(data("a"), data("1"), 100);
        Map<Data, CPMapEntry> entries = map.get();
        map.put(data("b"), data("2"), 200);
        assertEquals(1, entries.size());

        CPMapSnapshot snapshot = new CPMapSnapshot(Map.of("m", map.get()), Set.of("gone"));
        CPMapSnapshot copy = ss.toObject(ss.toData(snapshot));

        Map.Entry<String, Map<Data, CPMapEntry>> restored = copy.getValues().iterator().next();
        assertEquals("m", restored.getKey());
        CPMapContainer restoredMap = new CPMapContainer(groupId, "m", restored.getValue(), Long.MAX_VALUE);
        assertEquals(map.sizeBytes(), restoredMap.sizeBytes());
        assertEquals(data("2"), restoredMap.get(data("b")));
        assertEquals(200, restored.getValue().get(data("b")).timestamp());
        assertEquals(Set.of("gone"), copy.getDestroyed());

        restoredMap.remove(data("a"));
        assertFalse(map.get().isEmpty());
    }

    private Data data(Object o) {
        return ss.toData(o);
    }
}
