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

package uk.org.hecaton.internal.dynamicconfig;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.SplitBrainTestSupport;
import uk.org.hecaton.test.TestConfigUtils;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigSplitBrain_whenDifferentConfigExistsInBothBrainsTest extends SplitBrainTestSupport {

    private static final String MAP_NAME = "mapConfigWithNonDefaultName";

    @Override
    protected void onAfterSplitBrainCreated(HecatonInstance[] firstBrain, HecatonInstance[] secondBrain) {
        HecatonInstance instanceInBiggerBrain = firstBrain[0];
        MapConfig defaultMapConfig = new MapConfig(MAP_NAME);
        instanceInBiggerBrain.getConfig().addMapConfig(defaultMapConfig);

        HecatonInstance instanceInSmallerBrain = secondBrain[0];
        MapConfig mapConfig = new MapConfig(MAP_NAME)
                .setInMemoryFormat(TestConfigUtils.NON_DEFAULT_IN_MEMORY_FORMAT)
                .setBackupCount(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT);
        instanceInSmallerBrain.getConfig().addMapConfig(mapConfig);
    }

    @Override
    protected void onAfterSplitBrainHealed(HecatonInstance[] instances) {
        MapConfig defaultMapConfig = new Config().findMapConfig("default");

        for (HecatonInstance instance : instances) {
            MapConfig mapConfig = instance.getConfig().findMapConfig(MAP_NAME);
            assertEquals(defaultMapConfig.getInMemoryFormat(), mapConfig.getInMemoryFormat());
            assertEquals(defaultMapConfig.getBackupCount(), mapConfig.getBackupCount());
        }
    }
}
