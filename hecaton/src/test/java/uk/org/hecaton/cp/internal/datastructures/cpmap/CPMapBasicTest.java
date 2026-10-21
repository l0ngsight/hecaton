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

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPDataStructureManagementService;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.CPMap;
import uk.org.hecaton.cp.CPMapPurgeResponse;
import uk.org.hecaton.cp.internal.datastructures.cpmap.proxy.CPMapProxy;
import uk.org.hecaton.internal.util.RandomPicker;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static uk.org.hecaton.cp.CPGroup.DEFAULT_GROUP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPMapBasicTest extends AbstractCPMapBasicTest {

    @Override
    protected HecatonInstance[] createInstances() {
        return newInstances(3, 3, 1);
    }

    @Override
    protected <K, V> CPMap<K, V> createMap(String name) {
        HecatonInstance instance = instances[RandomPicker.getInt(instances.length)];
        return instance.getCPSubsystem().getMap(name);
    }

    @Test
    public void testCreate_withDefaultGroup() {
        CPMap<String, String> m = createMap(randomName());
        CPGroupId groupId = ((CPMapProxy<?, ?>) m).getGroupId();
        assertEquals(DEFAULT_GROUP_NAME, groupId.getName());
    }

    @Test
    public void testPurge() throws Exception {
        CPMap<String, String> purgeable = createMap(PURGEABLE_MAP_PREFIX + "Map");
        purgeable.set("old", "v");
        sleepMillis(3000);
        purgeable.set("new", "v");

        CPMapPurgeResponse response = managementService().purgeCPMap(PURGEABLE_MAP_PREFIX + "Map", Duration.ofMillis(1500))
                .toCompletableFuture().get();

        assertEquals(1, response.purgedCount());
        assertTrue(response.oldestRemainingEntryTimestamp() > 0);
        assertNull(purgeable.get("old"));
        assertEquals("v", purgeable.get("new"));
    }

    @Test
    public void testPurge_whenNothingRemains() throws Exception {
        CPMap<String, String> purgeable = createMap(PURGEABLE_MAP_PREFIX + "Empty");
        purgeable.set("k", "v");
        purgeable.delete("k");

        CPMapPurgeResponse response = managementService().purgeCPMap(PURGEABLE_MAP_PREFIX + "Empty", Duration.ZERO)
                .toCompletableFuture().get();

        assertEquals(0, response.purgedCount());
        assertEquals(-1, response.oldestRemainingEntryTimestamp());
    }

    @Test
    public void testPurge_whenNotEnabled() {
        map.set("k", "v");
        Throwable t = purgeFailure("map@group");
        assertTrue(t.toString(), t instanceof UnsupportedOperationException);
    }

    @Test
    public void testPurge_whenGroupDoesNotExist() {
        Throwable t = purgeFailure(PURGEABLE_MAP_PREFIX + "Map@noSuchGroup");
        assertTrue(t.toString(), t instanceof NoSuchElementException);
    }

    @Test
    public void testPurge_whenMapDoesNotExist() {
        map.set("k", "v");
        Throwable t = purgeFailure(PURGEABLE_MAP_PREFIX + "NeverCreated@group");
        assertTrue(t.toString(), t instanceof NoSuchElementException);
    }

    @Test(expected = NullPointerException.class)
    public void testPurge_nullName() {
        managementService().purgeCPMap(null, Duration.ZERO);
    }

    private CPDataStructureManagementService managementService() {
        return instances[0].getCPSubsystem().getCPDataStructureManagementService();
    }

    private Throwable purgeFailure(String mapName) {
        try {
            managementService().purgeCPMap(mapName, Duration.ZERO).toCompletableFuture().get();
            throw new AssertionError("purge of " + mapName + " succeeded");
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            while (t instanceof CompletionException || t instanceof ExecutionException) {
                t = t.getCause();
            }
            return t;
        }
    }
}
