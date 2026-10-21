/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.cp.internal.datastructures.atomicref;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPGroup;
import uk.org.hecaton.cp.CPGroup.CPGroupStatus;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.IAtomicReference;
import uk.org.hecaton.cp.exception.CPGroupDestroyedException;
import uk.org.hecaton.cp.internal.RaftInvocationManager;
import uk.org.hecaton.cp.internal.raftop.metadata.GetRaftGroupOp;
import uk.org.hecaton.cp.internal.raftop.metadata.TriggerDestroyRaftGroupOp;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.internal.util.RandomPicker;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.cp.CPGroup.DEFAULT_GROUP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicRefBasicTest extends AbstractAtomicRefBasicTest {

    @Override
    protected HecatonInstance[] createInstances() {
        return newInstances(3, 3, 1);
    }

    @Override
    protected String getName() {
        return "ref@group";
    }

    @Override
    protected IAtomicReference<String> createAtomicRef(String name) {
        HecatonInstance instance = instances[RandomPicker.getInt(instances.length)];
        return instance.getCPSubsystem().getAtomicReference(name);
    }

    @Test
    public void testCreate_withDefaultGroup() {
        IAtomicReference<String> atomicRef = createAtomicRef(randomName());
        assertEquals(DEFAULT_GROUP_NAME, getGroupId(atomicRef).getName());
    }

    @Test
    public void testRecreate_afterGroupDestroy() throws Exception {
        atomicRef.destroy();

        CPGroupId groupId = getGroupId(atomicRef);
        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        invocationManager.invoke(getRaftService(instances[0]).getMetadataGroupId(), new TriggerDestroyRaftGroupOp(groupId)).get();

        assertTrueEventually(() -> {
            CPGroup group = invocationManager.<CPGroup>invoke(getMetadataGroupId(instances[0]), new GetRaftGroupOp(groupId)).join();
            assertEquals(CPGroupStatus.DESTROYED, group.status());
        });

        try {
            atomicRef.get();
            fail();
        } catch (CPGroupDestroyedException ignored) {
        }

        atomicRef = createAtomicRef(name);
        assertNotEquals(groupId, getGroupId(atomicRef));

        atomicRef.set("str1");
    }
}
