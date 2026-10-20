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

package uk.org.hecaton.test.mocknetwork;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.test.Accessors.getAddress;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MockJoinerTest extends HecatonTestSupport {

    private int nodeCount = 5;

    @Test
    public void serialJoin() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance[] instances = new HecatonInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHecatonInstance();
        }

        assertClusterSizeEventually(instances);
    }

    @Test
    public void serialJoin_withNewInstances() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance[] instances = factory.newInstances(new Config(), nodeCount);

        assertClusterSizeEventually(instances);
    }

    @Test
    public void parallelJoin() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        final Collection<HecatonInstance> instances = Collections.synchronizedCollection(new ArrayList<>());

        final CountDownLatch latch = new CountDownLatch(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            new Thread(() -> {
                HecatonInstance hz = factory.newHecatonInstance();
                instances.add(hz);
                latch.countDown();
            }).start();
        }

        assertOpenEventually(latch);
        assertEquals(nodeCount, instances.size());

        for (HecatonInstance instance : instances) {
            assertClusterSizeEventually(nodeCount, instance);
        }
    }

    @Test
    public void restart_master() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance[] instances = factory.newInstances(new Config(), nodeCount);
        assertClusterSizeEventually(instances);

        Address address = getAddress(instances[0]);
        instances[0].getLifecycleService().terminate();

        instances[0] = factory.newHecatonInstance(address, new Config());
        assertClusterSizeEventually(instances);
    }

    @Test
    public void restart_slave() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance[] instances = factory.newInstances(new Config(), nodeCount);
        assertClusterSizeEventually(instances);

        Address address = getAddress(instances[1]);
        instances[1].getLifecycleService().terminate();

        instances[1] = factory.newHecatonInstance(address, new Config());
        assertClusterSizeEventually(instances);
    }

    @Test
    public void restart_multipleInParallel() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        final HecatonInstance[] instances = factory.newInstances(new Config(), nodeCount);
        assertClusterSizeEventually(instances);

        int restartCount = nodeCount / 2;
        final CountDownLatch latch = new CountDownLatch(restartCount);

        for (int i = 0; i < restartCount; i++) {
            final int ix = i;
            new Thread(() -> {
                Address address = getAddress(instances[ix]);
                instances[ix].getLifecycleService().terminate();
                instances[ix] = factory.newHecatonInstance(address, new Config());
                latch.countDown();
            }).start();
        }

        assertOpenEventually(latch);
        assertClusterSizeEventually(instances);
    }

    private void assertClusterSizeEventually(HecatonInstance... instances) {
        int size = instances.length;
        for (HecatonInstance instance : instances) {
            assertClusterSizeEventually(size, instance);
        }
    }
}
