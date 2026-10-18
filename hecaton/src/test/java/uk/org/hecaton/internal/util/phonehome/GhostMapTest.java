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

package uk.org.hecaton.internal.util.phonehome;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GhostMapTest extends HecatonTestSupport {

    /**
     * Verifies that GH issue https://github.com/hazelcast/hazelcast/issues/19833 is fixed, and PhoneHome does not create a map
     * called `asterisk#team*` if map `asterisk#team1` already exists (in this case the
     * node.config.mapConfigs.get("asterisk#team1") call returned a MapConfig that had the name `asterisk#team*`, and the name of
     * the map config was in turn used later to fetch a map (for obtaining its LocalMapStats).
     */
    @Test
    public void test_GH_19833() {
        HecatonInstance instance = createHecatonInstance(new Config()
                .addMapConfig(new MapConfig()
                        .setName("asterisk#team*")
                        .setTimeToLiveSeconds(10)
                )
        );
        instance.getMap("asterisk#team1");

        new PhoneHome(TestUtil.getNode(instance)).phoneHome(true);

        List<String> actualObjNames = instance.getDistributedObjects().stream().map(
                DistributedObject::getName).collect(toList());
        assertEquals(singletonList("asterisk#team1"), actualObjNames);
    }

    @Test
    public void phone_home_does_not_create_new_map_config() {
        String mapName = "hello";

        HecatonInstance instance = createHecatonInstance(new Config());
        instance.getMap(mapName);

        new PhoneHome(TestUtil.getNode(instance)).phoneHome(true);

        Set<String> mapNames = instance.getConfig().getMapConfigs().keySet();

        String failureMessage = "expected: [default] found: " + mapNames;
        assertEquals(failureMessage, 1, mapNames.size());
        assertTrue(failureMessage, mapNames.contains("default"));
        assertFalse(failureMessage, mapNames.contains(mapName));
    }
}
