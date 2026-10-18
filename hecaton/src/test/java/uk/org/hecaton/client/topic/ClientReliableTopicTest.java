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
import uk.org.hecaton.client.config.ClientConnectionStrategyConfig;
import uk.org.hecaton.client.impl.proxy.ClientReliableTopicProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.topic.ITopic;
import uk.org.hecaton.topic.Message;
import uk.org.hecaton.topic.MessageListener;
import uk.org.hecaton.topic.impl.reliable.DurableSubscriptionTest;
import uk.org.hecaton.topic.impl.reliable.ReliableMessageListenerMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientReliableTopicTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    private HecatonInstance client;

    @Before
    public void setup() {
        Config config = new Config();
        hecatonFactory.newHecatonInstance(config);
        client = hecatonFactory.newHecatonClient();
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testCreation() {
        ITopic topic = client.getReliableTopic(randomString());
        assertInstanceOf(ClientReliableTopicProxy.class, topic);
    }

    // ============== addMessageListener ==============================

    @Test
    public void addMessageListener() {
        ITopic topic = client.getReliableTopic(randomString());
        UUID id = topic.addMessageListener(new ReliableMessageListenerMock());
        assertNotNull(id);
    }

    // ============== removeMessageListener ==============================

    @Test
    public void removeMessageListener_whenExisting() {
        ITopic topic = client.getReliableTopic(randomString());
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        UUID id = topic.addMessageListener(listener);

        boolean removed = topic.removeMessageListener(id);
        assertTrue(removed);
        assertTrue(listener.onCancelCalled);
        topic.publish("1");

        // it should not receive any events.
        assertTrueDelayed5sec(() -> assertEquals(0, listener.objects.size()));
    }

    @Test
    public void removeMessageListener_whenNonExisting() {
        ITopic topic = client.getReliableTopic(randomString());
        boolean result = topic.removeMessageListener(UUID.randomUUID());

        assertFalse(result);
    }

    @Test
    public void removeMessageListener_whenAlreadyRemoved() {
        ITopic topic = client.getReliableTopic(randomString());
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        UUID id = topic.addMessageListener(listener);
        topic.removeMessageListener(id);

        boolean result = topic.removeMessageListener(id);
        assertFalse(result);

        topic.publish("1");

        // it should not receive any events.
        assertTrueDelayed5sec(() -> assertEquals(0, listener.objects.size()));
    }

    // ============================================

    @Test
    public void publishSingle() {
        ITopic topic = client.getReliableTopic(randomString());
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        topic.addMessageListener(listener);
        final String msg = "foobar";
        topic.publish(msg);

        assertTrueEventually(() -> assertContains(listener.objects, msg));
    }

    @Test
    public void publishMultiple() {
        ITopic topic = client.getReliableTopic(randomString());
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        topic.addMessageListener(listener);

        final List<String> items = new ArrayList<>();
        for (int k = 0; k < 5; k++) {
            items.add("" + k);
        }

        for (String item : items) {
            topic.publish(item);
        }

        assertTrueEventually(() -> assertEquals(items, Arrays.asList(listener.objects.toArray())));
    }

    @Test
    public void testMessageFieldSetCorrectly() {
        ITopic topic = client.getReliableTopic(randomString());
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        topic.addMessageListener(listener);

        final long beforePublishTime = Clock.currentTimeMillis();
        final String messageStr = randomString();
        topic.publish(messageStr);
        final long afterPublishTime = Clock.currentTimeMillis();

        assertTrueEventually(() -> {
            assertEquals(1, listener.messages.size());
            Message<String> message = listener.messages.get(0);

            assertEquals(messageStr, message.getMessageObject());
            assertNull(message.getPublishingMember());

            long actualPublishTime = message.getPublishTime();
            assertTrue(actualPublishTime >= beforePublishTime);
            assertTrue(actualPublishTime <= afterPublishTime);
        });
    }

    // makes sure that when a listener is register, we don't see any messages being published before
    // it got registered. We'll only see the messages after it got registered.
    @Test
    public void testAlwaysStartAfterTail() {
        final ITopic topic = client.getReliableTopic(randomString());
        topic.publish("1");
        topic.publish("2");
        topic.publish("3");

        spawn((Runnable) () -> {
            sleepSeconds(5);
            topic.publish("4");
            topic.publish("5");
            topic.publish("6");
        });

        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        topic.addMessageListener(listener);

        assertTrueEventually(() -> assertEquals(asList("4", "5", "6"), Arrays.asList(listener.objects.toArray())));
        assertFalse(listener.onCancelCalled);
    }

    @Test
    public void testListener() throws InterruptedException {
        ITopic topic = client.getReliableTopic(randomString());
        int messageCount = 10;
        final CountDownLatch latch = new CountDownLatch(messageCount);
        MessageListener listener = message -> latch.countDown();
        topic.addMessageListener(listener);

        for (int i = 0; i < messageCount; i++) {
            topic.publish(i);
        }
        assertTrue(latch.await(20, TimeUnit.SECONDS));
    }

    @Test
    public void testRemoveListener() {
        ITopic topic = client.getReliableTopic(randomString());

        MessageListener listener = message -> {
        };
        UUID id = topic.addMessageListener(listener);

        assertTrue(topic.removeMessageListener(id));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalTopicStats() {
        ITopic topic = client.getReliableTopic(randomString());

        topic.getLocalTopicStats();
    }

    @Test
    public void shouldNotBeTerminated_whenClientIsOffline() {
        final HecatonInstance ownerMember = hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.ASYNC);
        String topicName = "topic";
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        int publishCount = 1000;

        final CountDownLatch messageArrived = new CountDownLatch(publishCount);
        ITopic<String> topic = client.getReliableTopic(topicName);
        final UUID id = topic.addMessageListener(new DurableSubscriptionTest.DurableMessageListener<>() {
            @Override
            public void onMessage(Message<String> message) {
                messageArrived.countDown();
            }

        });

        HecatonInstance member2 = hecatonFactory.newHecatonInstance();
        waitAllForSafeState(ownerMember, member2);

        ITopic<Object> reliableTopic = member2.getReliableTopic(topicName);

        //kill the the owner member, while messages are coming
        new Thread(() -> {
            sleepMillis(1);
            ownerMember.shutdown();
        }).start();

        for (int i = 0; i < publishCount; i++) {
            reliableTopic.publish("msg " + (i + 100));
        }

        assertOpenEventually(messageArrived);
        assertTrue(topic.removeMessageListener(id));
    }

    @Test
    public void testOnCancelCalledWhenExceptionOccurs() {
        ITopic topic = client.getReliableTopic(randomString());
        ReliableMessageListenerMock listener = new ReliableMessageListenerMock() {
            @Override
            public void onMessage(Message message) {
                throw new RuntimeException("This exception should cause onCancel method to be called!");
            }
        };

        topic.addMessageListener(listener);

        topic.publish("foo");

        assertTrueEventually(() -> assertTrue(listener.onCancelCalled));
    }
}
