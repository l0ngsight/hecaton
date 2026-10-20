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

import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.impl.queue.model.VersionedObject;
import uk.org.hecaton.collection.impl.queue.model.VersionedObjectComparator;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.DistributedObjectEvent;
import uk.org.hecaton.core.DistributedObjectListener;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.exception.DistributedObjectDestroyedException;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueEvictionTest extends HecatonTestSupport {

    @Parameterized.Parameters(name = "comparatorClassName: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{null, VersionedObjectComparator.class.getName()});
    }

    @Parameterized.Parameter
    public String comparatorClassName;

    private String queueName = randomString();

    @Test
    public void whenTtlIsSet_andQueueIsDrained_thenTakeThrowsException() throws Exception {
        Config config = smallInstanceConfig();
        config.getQueueConfig(queueName)
              .setPriorityComparatorClassName(comparatorClassName)
              .setEmptyQueueTtl(2);
        HecatonInstance hz = createHecatonInstance(config);
        IQueue<VersionedObject<String>> queue = hz.getQueue(queueName);

        // Add & remove an item
        assertTrue(queue.offer(getItem()));
        assertEquals(getItem(), queue.poll());

        // Once empty, the queue should _immediately_ dispose
        assertThatThrownBy(queue::take).isInstanceOf(DistributedObjectDestroyedException.class);
        assertThat(queue).isEmpty();
    }

    @Test
    public void whenTtlIsZero_thenListenersAreNeverthelessExecuted() throws Exception {
        Config config = smallInstanceConfig();
        config.getQueueConfig("default")
              .setPriorityComparatorClassName(comparatorClassName);
        config.getQueueConfig(queueName).setEmptyQueueTtl(0);
        HecatonInstance hz = createHecatonInstance(config);

        final CountDownLatch latch = new CountDownLatch(2);
        hz.addDistributedObjectListener(new DistributedObjectListener() {
            @Override
            public void distributedObjectCreated(DistributedObjectEvent event) {
                latch.countDown();
            }

            @Override
            public void distributedObjectDestroyed(DistributedObjectEvent event) {
                latch.countDown();
            }
        });

        IQueue<VersionedObject<String>> queue = hz.getQueue(queueName);

        // Add & remove an item from
        assertTrue(queue.offer(getItem()));
        assertEquals(getItem(), queue.poll());

        // Check that (eventually) the queue is disposed
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    private static VersionedObject<String> getItem() {
        return new VersionedObject<>("item");
    }
}
