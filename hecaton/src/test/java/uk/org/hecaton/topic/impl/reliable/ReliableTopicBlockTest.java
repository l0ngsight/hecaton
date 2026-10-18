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

package uk.org.hecaton.topic.impl.reliable;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ReliableTopicConfig;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.topic.ITopic;
import uk.org.hecaton.topic.TopicOverloadPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReliableTopicBlockTest extends HecatonTestSupport {

    private HecatonInstance local;
    private ITopic<String> topic;

    @Before
    public void setup() {
        Config config = smallInstanceConfig();
        config.addRingBufferConfig(new RingbufferConfig("blockingReliableTopic*")
                .setCapacity(10)
                .setTimeToLiveSeconds(60));

        config.addReliableTopicConfig(new ReliableTopicConfig("blockingReliableTopic*")
                .setReadBatchSize(10)
                .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK));

        HecatonInstance[] instances = createHecatonInstanceFactory(1).newInstances(config);
        local = instances[0];
        HecatonInstance target = instances[instances.length - 1];

        String name = randomNameOwnedBy(target, "reliableTopic");
        topic = local.getReliableTopic(name);
    }

    @Test
    public void testBlockingAsync() {
        AtomicInteger count = new AtomicInteger();
        topic.addMessageListener(message -> count.incrementAndGet());
        for (int i = 0; i < 10; i++) {
            topic.publish("message");
        }
        assertTrueEventually(() -> assertEquals(10, count.get()));
        final List<String> data = Arrays.asList("msg 1", "msg 2", "msg 3", "msg 4", "msg 5");
        assertCompletesEventually(topic.publishAllAsync(data).toCompletableFuture());
        assertTrueEventually(() -> assertEquals(15, count.get()));
    }
}
