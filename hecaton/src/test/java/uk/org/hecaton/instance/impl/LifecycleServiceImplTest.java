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

package uk.org.hecaton.instance.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@RunWith(HecatonSerialClassRunner.class)
@Category(NightlyTest.class)
public class LifecycleServiceImplTest extends JetTestSupport {

    @Test
    // test for https://github.com/hazelcast/hazelcast/issues/21123
    public void testConcurrentShutdown() throws Exception {
        for (int i = 0; i < 3; i++) {
            HecatonInstance inst = createHecatonInstance();

            Future<?> f1 = spawn(() -> inst.getLifecycleService().terminate());
            Future<?> f2 = spawn(() -> inst.getLifecycleService().shutdown());

            f1.get(5, TimeUnit.SECONDS);
            f2.get(5, TimeUnit.SECONDS);
        }
    }
}
