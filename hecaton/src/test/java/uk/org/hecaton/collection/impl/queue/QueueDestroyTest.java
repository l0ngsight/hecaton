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

package uk.org.hecaton.collection.impl.queue;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.function.Supplier;

import static uk.org.hecaton.collection.impl.queue.QueueService.SERVICE_NAME;
import static org.junit.Assert.assertEquals;


@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueDestroyTest extends HecatonTestSupport {

    private static final int LOOP_COUNT = 10;
    private static final int ADD_COUNT = 100;

    private static final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @Parameterized.Parameter
    public Supplier<HecatonInstance> instanceSupplier;

    @Parameterized.Parameters(name = "instance={0}")
    public static Supplier<HecatonInstance>[] parameters() {
        return new Supplier[]{hecatonFactory::newHecatonClient,
                () -> hecatonFactory.newHecatonInstance(smallInstanceConfig())};
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void checkStatsMapEntryRemovedWhenQueueDestroyed() {
        HecatonInstance member = hecatonFactory.newHecatonInstance(smallInstanceConfig());
        HecatonInstance destroyerInstance = instanceSupplier.get();
        QueueService qService = Accessors.getService(member, SERVICE_NAME);

        for (int i = 0; i < LOOP_COUNT; i++) {
            String queueName = String.valueOf(i);
            IQueue<Integer> q = destroyerInstance.getQueue(queueName);

            for (int j = 0; j < ADD_COUNT; j++) {
                q.add(j);
            }
            q.destroy();
            assertEquals(0, qService.getStatsMap().size());
        }
    }
}
