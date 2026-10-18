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

package uk.org.hecaton.internal.management.dto;

import uk.org.hecaton.config.ConfigCompatibilityChecker.WanSyncConfigChecker;
import uk.org.hecaton.config.ConsistencyCheckStrategy;
import uk.org.hecaton.config.WanSyncConfig;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanSyncConfigDTOTest {

    private static final WanSyncConfigChecker WAN_SYNC_CONFIG_CHECKER = new WanSyncConfigChecker();

    @Test
    public void testSerialization() {
        WanSyncConfig expected = new WanSyncConfig()
                .setConsistencyCheckStrategy(ConsistencyCheckStrategy.MERKLE_TREES);

        WanSyncConfig actual = cloneThroughJson(expected);
        assertTrue("Expected: " + expected + ", got:" + actual,
                WAN_SYNC_CONFIG_CHECKER.check(expected, actual));
    }

    @Test
    public void testDefault() {
        WanSyncConfig expected = new WanSyncConfig();

        WanSyncConfig actual = cloneThroughJson(expected);
        assertTrue("Expected: " + expected + ", got:" + actual,
                WAN_SYNC_CONFIG_CHECKER.check(expected, actual));
    }

    private WanSyncConfig cloneThroughJson(WanSyncConfig expected) {
        WanSyncConfigDTO dto = new WanSyncConfigDTO(expected);

        JsonObject json = dto.toJson();
        WanSyncConfigDTO deserialized = new WanSyncConfigDTO(null);
        deserialized.fromJson(json);

        return deserialized.getConfig();
    }

}
