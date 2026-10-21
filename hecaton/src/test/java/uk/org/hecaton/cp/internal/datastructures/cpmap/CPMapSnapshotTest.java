/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.cp.internal.datastructures.cpmap;

import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.CPMap;
import uk.org.hecaton.cp.internal.RaftOp;
import uk.org.hecaton.cp.internal.datastructures.AbstractAtomicRegisterSnapshotTest;
import uk.org.hecaton.cp.internal.datastructures.cpmap.operation.GetOp;
import uk.org.hecaton.cp.internal.datastructures.cpmap.proxy.CPMapProxy;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPMapSnapshotTest extends AbstractAtomicRegisterSnapshotTest<String> {

    private final String name = "map";
    private CPMap<String, String> map;

    @Before
    public void createProxy() {
        map = getCPSubsystem().getMap(name);
    }

    @Override
    protected CPGroupId getGroupId() {
        return ((CPMapProxy<?, ?>) map).getGroupId();
    }

    @Override
    protected String setAndGetInitialValue() {
        for (int i = 0; i < 10; i++) {
            map.set("other" + i, randomString());
        }
        String value = randomString();
        map.set("key", value);
        return value;
    }

    @Override
    protected RaftOp getQueryRaftOp() {
        return new GetOp(name, new DefaultSerializationServiceBuilder().build().toData("key"));
    }
}
