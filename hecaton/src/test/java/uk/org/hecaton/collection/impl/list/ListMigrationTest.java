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

package uk.org.hecaton.collection.impl.list;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ListMigrationTest extends HecatonTestSupport {

    private List list;
    private HecatonInstance local;
    private HecatonInstance remote1;
    private HecatonInstance remote2;

    @Before
    public void setup() {
        HecatonInstance[] cluster = createHecatonInstanceFactory(3).newInstances();
        local = cluster[0];
        remote1 = cluster[1];
        remote2 = cluster[2];

        String name = randomNameOwnedBy(remote1);
        list = local.getList(name);
    }

    @Test
    public void test() {
        for (int k = 0; k < 100; k++) {
            list.add(k);
        }

        remote1.shutdown();
        remote2.shutdown();

        assertEquals(100, list.size());

        for (int k = 0; k < 100; k++) {
            assertEquals("the set doesn't contain:" + k, k, list.get(k));
        }
    }
}
