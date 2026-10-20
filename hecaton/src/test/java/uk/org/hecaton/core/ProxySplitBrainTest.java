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

package uk.org.hecaton.core;

import uk.org.hecaton.jet.impl.JobRepository;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.SplitBrainTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProxySplitBrainTest extends SplitBrainTestSupport {

    @Override
    protected void onAfterSplitBrainCreated(HecatonInstance[] firstBrain, HecatonInstance[] secondBrain) {
        HecatonInstance isolatedInstance = secondBrain[0];
        isolatedInstance.getQueue("isolatedQ");
        assertDistributedObjectCountEventually(1, isolatedInstance);

        for (HecatonInstance hz : firstBrain) {
            String name = generateKeyOwnedBy(hz);
            hz.getQueue(name);
        }

        for (HecatonInstance hz : firstBrain) {
            int expectedCount = firstBrain.length;
            assertDistributedObjectCountEventually(expectedCount, hz);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HecatonInstance[] allInstances) {
        for (HecatonInstance hz : allInstances) {
            assertDistributedObjectCountEventually(allInstances.length, hz);
        }
    }

    private static void assertDistributedObjectCountEventually(final int expectedCount, final HecatonInstance hz) {
        assertTrueEventually(() -> {
            Collection<DistributedObject> distributedObjects = hz.getDistributedObjects().stream()
                    .filter(o -> !o.getName().startsWith(JobRepository.INTERNAL_JET_OBJECTS_PREFIX))
                    .toList();
            int actualSize = distributedObjects.size();
            assertEquals(expectedCount, actualSize);
        });
    }
}
