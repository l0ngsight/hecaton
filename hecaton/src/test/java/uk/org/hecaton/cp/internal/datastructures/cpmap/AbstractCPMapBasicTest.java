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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.cp.CPMapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPGroup;
import uk.org.hecaton.cp.CPMap;
import uk.org.hecaton.cp.internal.HecatonRaftTestSupport;
import uk.org.hecaton.spi.exception.DistributedObjectDestroyedException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractCPMapBasicTest extends HecatonRaftTestSupport {

    /** Maps whose names start with this have a 1 MB limit. */
    protected static final String SMALL_MAP_PREFIX = "small";
    /** Maps whose names start with this can be purged. */
    protected static final String PURGEABLE_MAP_PREFIX = "purgeable";

    protected HecatonInstance[] instances;
    protected CPMap<String, String> map;

    @Before
    public void setup() {
        instances = createInstances();
        map = createMap("map@group");
    }

    protected abstract HecatonInstance[] createInstances();

    protected abstract <K, V> CPMap<K, V> createMap(String name);

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.getCPSubsystemConfig()
              .addCPMapConfig(new CPMapConfig(SMALL_MAP_PREFIX + "*", 1))
              .addCPMapConfig(new CPMapConfig(PURGEABLE_MAP_PREFIX + "*").setPurgeEnabled(true));
        return config;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateProxyOnMetadataCPGroup() {
        createMap("map@" + CPGroup.METADATA_CP_GROUP_NAME);
    }

    @Test
    public void testPut() {
        assertNull(map.put("k", "v1"));
        assertEquals("v1", map.put("k", "v2"));
        assertEquals("v2", map.get("k"));
    }

    @Test
    public void testSet() {
        map.set("k", "v1");
        map.set("k", "v2");
        assertEquals("v2", map.get("k"));
    }

    @Test
    public void testPutIfAbsent() {
        assertNull(map.putIfAbsent("k", "v1"));
        assertEquals("v1", map.putIfAbsent("k", "v2"));
        assertEquals("v1", map.get("k"));
    }

    @Test
    public void testRemove() {
        assertNull(map.remove("k"));
        map.set("k", "v");
        assertEquals("v", map.remove("k"));
        assertNull(map.get("k"));
    }

    @Test
    public void testDelete() {
        map.delete("k");
        map.set("k", "v");
        map.delete("k");
        assertNull(map.get("k"));
    }

    @Test
    public void testCompareAndSet() {
        assertFalse(map.compareAndSet("k", "v1", "v2"));
        assertNull(map.get("k"));
        map.set("k", "v1");
        assertFalse(map.compareAndSet("k", "other", "v2"));
        assertEquals("v1", map.get("k"));
        assertTrue(map.compareAndSet("k", "v1", "v2"));
        assertEquals("v2", map.get("k"));
    }

    @Test
    public void testKeysAndValuesOfOtherTypes() {
        CPMap<Integer, Long> other = createMap("other@group");
        other.set(1, 10L);
        assertEquals(Long.valueOf(10L), other.get(1));
        assertNull(other.get(2));
    }

    @Test
    public void testMapsAreIndependent() {
        CPMap<String, String> other = createMap("other@group");
        map.set("k", "v");
        assertNull(other.get("k"));
    }

    @Test
    public void testSameMapFromAnotherProxy() {
        map.set("k", "v");
        CPMap<String, String> again = createMap("map@group");
        assertEquals("v", again.get("k"));
    }

    @Test(expected = NullPointerException.class)
    public void testPut_nullKey() {
        map.put(null, "v");
    }

    @Test(expected = NullPointerException.class)
    public void testSet_nullValue() {
        map.set("k", null);
    }

    @Test(expected = NullPointerException.class)
    public void testGet_nullKey() {
        map.get(null);
    }

    @Test(expected = NullPointerException.class)
    public void testCompareAndSet_nullExpectedValue() {
        map.compareAndSet("k", null, "v");
    }

    @Test
    public void testSizeLimit() {
        CPMap<String, byte[]> small = createMap(SMALL_MAP_PREFIX + "Map");
        byte[] value = new byte[300 * 1024];
        small.set("k1", value);
        small.set("k2", value);
        small.set("k3", value);
        assertThrows(IllegalStateException.class, () -> small.set("k4", value));
        assertNull(small.get("k4"));

        // replacing an entry with one of the same size fits, and removing one makes room
        small.set("k1", new byte[300 * 1024]);
        small.delete("k1");
        small.set("k4", value);
        assertEquals(value.length, small.get("k4").length);
    }

    @Test
    public void testDefaultSizeLimitAllowsLargeValues() {
        CPMap<String, byte[]> large = createMap("large");
        large.set("k", new byte[5 * 1024 * 1024]);
        assertEquals(5 * 1024 * 1024, large.get("k").length);
    }

    @Test
    public void testDestroy() {
        map.set("k", "v");
        map.destroy();
        assertThrows(DistributedObjectDestroyedException.class, () -> map.get("k"));
    }
}
