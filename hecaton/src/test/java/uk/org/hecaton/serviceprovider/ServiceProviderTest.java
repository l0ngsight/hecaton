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

package uk.org.hecaton.serviceprovider;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;


@RunWith(HecatonParallelClassRunner.class)
@Category(value = {QuickTest.class, ParallelJVMTest.class})
public class ServiceProviderTest extends HecatonTestSupport {
    @Test
    public void remoteServiceLoaderTest() {
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(1);
        HecatonInstance instance = nodeFactory.newHecatonInstance();
        TestDistributedObject testDistributedObject = instance.getDistributedObject(TestRemoteService.SERVICE_NAME, "test");
        assertEquals("test", testDistributedObject.getName());
    }
}
