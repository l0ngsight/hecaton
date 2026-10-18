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

package uk.org.hecaton.wan.impl;

import uk.org.hecaton.map.impl.wan.WanMapRemoveEvent;
import uk.org.hecaton.map.impl.wan.WanMapAddOrUpdateEvent;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanDataSerializerHookTest {

    @Test
    public void testExistingTypes() {
        WanDataSerializerHook hook = new WanDataSerializerHook();
        IdentifiedDataSerializable mapUpdate = hook.createFactory()
                .create(WanDataSerializerHook.MAP_REPLICATION_UPDATE);
        assertTrue(mapUpdate instanceof WanMapAddOrUpdateEvent);

        IdentifiedDataSerializable mapRemove = hook.createFactory()
                .create(WanDataSerializerHook.MAP_REPLICATION_REMOVE);
        assertTrue(mapRemove instanceof WanMapRemoveEvent);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidType() {
        WanDataSerializerHook hook = new WanDataSerializerHook();
        hook.createFactory().create(999);
    }
}
