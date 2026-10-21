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

package uk.org.hecaton.client.cp.internal.datastructures.atomiclong;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.IAtomicLong;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.internal.datastructures.atomiclong.AtomicLongBasicTest;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AtomicLongClientBasicTest extends AtomicLongBasicTest {

    private HecatonInstance client;

    @Override
    protected TestHecatonInstanceFactory createTestFactory() {
        return new TestHecatonFactory();
    }

    @Override
    protected HecatonInstance[] createInstances() {
        HecatonInstance[] instances = super.createInstances();
        TestHecatonFactory f = (TestHecatonFactory) factory;
        client = f.newHecatonClient();
        return instances;
    }

    @Override
    protected IAtomicLong createAtomicLong(String name) {
        return client.getCPSubsystem().getAtomicLong(name);
    }

    @After
    public void shutdown() {
        factory.terminateAll();
    }

    @Override
    protected CPGroupId getGroupId(IAtomicLong atomicLong) {
        return ((AtomicLongProxy) atomicLong).getGroupId();
    }

    @Test
    @Ignore
    public void testLocalGet_withLeaderLocalPolicy() {
    }

    @Test
    @Ignore
    public void testLocalGet_withAnyLocalPolicy() {
    }
}
