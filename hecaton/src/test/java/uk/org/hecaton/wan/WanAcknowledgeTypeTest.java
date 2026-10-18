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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.config.WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE;
import static uk.org.hecaton.config.WanAcknowledgeType.ACK_ON_RECEIPT;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanAcknowledgeTypeTest {

    @Test
    public void test() {
        assertSame(ACK_ON_RECEIPT, WanAcknowledgeType.getById(ACK_ON_RECEIPT.getId()));
        assertSame(ACK_ON_OPERATION_COMPLETE, WanAcknowledgeType.getById(ACK_ON_OPERATION_COMPLETE.getId()));
        assertNull(WanAcknowledgeType.getById(-1));
    }
}
