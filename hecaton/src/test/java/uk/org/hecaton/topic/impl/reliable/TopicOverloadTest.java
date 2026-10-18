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
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.topic.TopicOverloadPolicy;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.AbstractHecatonClassRunner.getTestMethodName;
import static uk.org.hecaton.test.Accessors.getSerializationService;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TopicOverloadTest extends TopicOverloadAbstractTest {

    @Before
    public void setupCluster() {
        Config config = new Config();
        config.addRingBufferConfig(new RingbufferConfig("when*")
                .setCapacity(100).setTimeToLiveSeconds(Integer.MAX_VALUE));
        config.addReliableTopicConfig(new ReliableTopicConfig("whenError_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.ERROR));
        config.addReliableTopicConfig(new ReliableTopicConfig("whenDiscardOldest_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST));
        config.addReliableTopicConfig(new ReliableTopicConfig("whenDiscardNewest_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_NEWEST));
        config.addReliableTopicConfig(new ReliableTopicConfig("whenBlock_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK));

        HecatonInstance hz = createHecatonInstance(config);

        serializationService = getSerializationService(hz);

        String topicName = getTestMethodName();
        topic = hz.getReliableTopic(topicName);

        ringbuffer = ((ReliableTopicProxy<String>) topic).ringbuffer;
    }


}
