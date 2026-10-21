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

import uk.org.hecaton.cp.IAtomicReference;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.internal.RaftOp;
import uk.org.hecaton.cp.internal.datastructures.AbstractAtomicRegisterSnapshotTest;
import uk.org.hecaton.cp.internal.datastructures.atomicref.operation.GetOp;
import uk.org.hecaton.cp.internal.datastructures.atomicref.proxy.AtomicRefProxy;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicRefSnapshotTest extends AbstractAtomicRegisterSnapshotTest<String> {

    private IAtomicReference<String> atomicRef;
    private String name = "ref";

    @Before
    public void createProxy() {
        atomicRef = getCPSubsystem().getAtomicReference(name);
    }

    @Override
    protected CPGroupId getGroupId() {
        return ((AtomicRefProxy) atomicRef).getGroupId();
    }

    @Override
    protected String setAndGetInitialValue() {
        String value = randomString();
        atomicRef.set(value);
        return value;
    }

    @Override
    protected RaftOp getQueryRaftOp() {
        return new GetOp(name);
    }
}
