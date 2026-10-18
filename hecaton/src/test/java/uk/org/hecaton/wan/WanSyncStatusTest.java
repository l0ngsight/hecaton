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

package uk.org.hecaton.wan;

import uk.org.hecaton.config.WanAcknowledgeType;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.wan.impl.WanSyncStatus;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.wan.impl.WanSyncStatus.FAILED;
import static uk.org.hecaton.wan.impl.WanSyncStatus.IN_PROGRESS;
import static uk.org.hecaton.wan.impl.WanSyncStatus.READY;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanSyncStatusTest {

    @Test
    public void test() {
        assertSame(READY, WanSyncStatus.getByStatus(READY.getStatus()));
        assertSame(IN_PROGRESS, WanSyncStatus.getByStatus(IN_PROGRESS.getStatus()));
        assertSame(FAILED, WanSyncStatus.getByStatus(FAILED.getStatus()));
        assertNull(WanAcknowledgeType.getById(-1));
    }
}
