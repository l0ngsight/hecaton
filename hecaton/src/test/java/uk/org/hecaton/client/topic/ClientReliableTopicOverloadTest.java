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

package uk.org.hecaton.client.topic;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientReliableTopicConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.proxy.ClientReliableTopicProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.topic.TopicOverloadPolicy;
import uk.org.hecaton.topic.impl.reliable.TopicOverloadAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.AbstractHecatonClassRunner.getTestMethodName;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientReliableTopicOverloadTest extends TopicOverloadAbstractTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @Before
    public void setupCluster() {
        Config config = new Config();
        config.addRingBufferConfig(new RingbufferConfig("when*")
                .setCapacity(100).setTimeToLiveSeconds(Integer.MAX_VALUE));
        hecatonFactory.newHecatonInstance(config);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addReliableTopicConfig(new ClientReliableTopicConfig("whenError_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.ERROR));
        clientConfig.addReliableTopicConfig(new ClientReliableTopicConfig("whenDiscardOldest_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST));
        clientConfig.addReliableTopicConfig(new ClientReliableTopicConfig("whenDiscardNewest_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_NEWEST));
        clientConfig.addReliableTopicConfig(new ClientReliableTopicConfig("whenBlock_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK));
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        serializationService = ((HecatonClientProxy) client).getSerializationService();

        String topicName = getTestMethodName();
        topic = client.getReliableTopic(topicName);

        ringbuffer = ((ClientReliableTopicProxy<String>) topic).getRingbuffer();
    }

    @After
    public void terminate() {
        hecatonFactory.terminateAll();
    }


}
