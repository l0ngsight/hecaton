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

package uk.org.hecaton.map.impl.eviction;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.OverridePropertyRule;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.config.EvictionPolicy.LFU;
import static uk.org.hecaton.test.OverridePropertyRule.set;
import static org.junit.Assert.assertEquals;


@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class})
public class PluggableMemoryInfoAccessorTest extends HecatonTestSupport {

    private static final String HECATON_MEMORY_INFO_ACCESSOR_IMPL = "hecaton.memory.info.accessor.impl";

    @Rule
    public final OverridePropertyRule overridePropertyRule = set(HECATON_MEMORY_INFO_ACCESSOR_IMPL,
            ZeroMemoryInfoAccessor.class.getCanonicalName());


    /**
     * Used {@link ZeroMemoryInfoAccessor} to evict every put, map should not contain any entry after this test run.
     */
    @Test
    public void testPluggedMemoryInfoAccessorUsed() {
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig("test");
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig
                .setEvictionPolicy(LFU)
                .setMaxSizePolicy(MaxSizePolicy.FREE_HEAP_PERCENTAGE)
                .setSize(50);

        HecatonInstance node = createHecatonInstance(config);
        IMap<Integer, Integer> map = node.getMap("test");

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        assertEquals(0, map.size());
    }


}
