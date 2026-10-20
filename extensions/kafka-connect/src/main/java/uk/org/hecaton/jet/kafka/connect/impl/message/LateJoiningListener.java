/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.jet.kafka.connect.impl.message;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.ringbuffer.impl.RingbufferService;
import uk.org.hecaton.topic.MessageListener;
import uk.org.hecaton.topic.impl.reliable.ReliableMessageListenerAdapter;

/**
 * This listener will get the last published message if it joins late.
 */
class LateJoiningListener<E> extends ReliableMessageListenerAdapter<E> {
    private final HecatonInstance hecatonInstance;
    private final String topicName;

    LateJoiningListener(HecatonInstance hecatonInstance, String topicName, MessageListener<E> messageListener) {
        super(messageListener);
        this.hecatonInstance = hecatonInstance;
        this.topicName = topicName;
    }

    @Override
    public long retrieveInitialSequence() {
        Ringbuffer<Object> ringbuffer = hecatonInstance.getRingbuffer(RingbufferService.TOPIC_RB_PREFIX + topicName);
        // We return either the current tail sequence - we will get the last message,
        // or we return 0 - we will get the first message in the topic
        // We must not return -1, because we could miss a message sent concurrently with
        // the initialization of the listener
        return Math.max(ringbuffer.tailSequence(), 0);
    }
}
