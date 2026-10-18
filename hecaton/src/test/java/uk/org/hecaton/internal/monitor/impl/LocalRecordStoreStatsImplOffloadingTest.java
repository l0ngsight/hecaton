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

package uk.org.hecaton.internal.monitor.impl;

import uk.org.hecaton.client.map.helpers.AMapStore;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.OverridePropertyRule;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static uk.org.hecaton.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static uk.org.hecaton.test.OverridePropertyRule.set;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalRecordStoreStatsImplOffloadingTest extends HecatonTestSupport {

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule
            = set(PROP_TASK_PERIOD_SECONDS, String.valueOf(1));

    String MAP_NAME = "map-name";

    private IMap<String, String> map;

    @Before
    public void setUp() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setMapStoreConfig(new MapStoreConfig()
                .setClassName(AMapStore.class.getName()));
        config.setMapConfigs(Map.of(MAP_NAME, mapConfig));

        HecatonInstance instance = createHecatonInstance(config);
        map = instance.getMap(MAP_NAME);
    }

    @Test
    public void delete_updates_last_update_time() {
        for (int i = 0; i < 10; i++) {
            map.set("key:" + i, "value:" + i);
        }

        assertLastUpdateTimeUpdated(() -> map.delete("key:0"));
    }

    @Test
    public void delete_does_not_update_last_update_time_when_no_matching_key() {
        for (int i = 0; i < 10; i++) {
            map.set("key:" + i, "value:" + i);
        }

        assertLastUpdateTimeIsNotUpdated(() -> map.delete("key:100"));
    }

    private void assertLastUpdateTime(Runnable runnable, boolean updated) {
        long lastUpdateTimeBefore = map.getLocalMapStats().getLastUpdateTime();
        sleepAtLeastMillis(100);

        runnable.run();

        long lastUpdateTimeAfter = map.getLocalMapStats().getLastUpdateTime();
        if (updated) {
            assertThat(lastUpdateTimeAfter).isGreaterThan(lastUpdateTimeBefore);
        } else {
            assertThat(lastUpdateTimeAfter).isEqualTo(lastUpdateTimeBefore);
        }
    }

    private void assertLastUpdateTimeUpdated(Runnable runnable) {
        assertLastUpdateTime(runnable, true);
    }

    private void assertLastUpdateTimeIsNotUpdated(Runnable runnable) {
        assertLastUpdateTime(runnable, false);
    }
}
