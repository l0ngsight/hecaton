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
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.topic.Message;
import uk.org.hecaton.test.ExpectedRuntimeException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ErrorHandlingTest extends HecatonTestSupport {

    private ReliableTopicProxy<String> topic;

    @Before
    public void setup() {
        Config config = new Config();
        config.addRingBufferConfig(new RingbufferConfig("foo")
                .setCapacity(100)
                .setTimeToLiveSeconds(0));
        HecatonInstance hz = createHecatonInstance(config);
        topic = (ReliableTopicProxy<String>) hz.<String>getReliableTopic("foo");
    }

    @Test
    public void isTerminal_throwsException_thenTerminate() {
        final ErrorListenerMock listener = new ErrorListenerMock();
        listener.throwErrorOnIsTerminal = true;
        listener.isTerminal = true;
        topic.addMessageListener(listener);

        topic.publish("item1");

        assertTrueEventually(() -> {
            assertEquals(1, listener.objects.size());
            assertTrue(topic.runnersMap.isEmpty());
        });

        topic.publish("item2");

        // we need to make sure we don't receive item 2 since the listener is terminated
        assertTrueFiveSeconds(() -> assertEquals(1, listener.objects.size()));
    }

    @Test
    public void whenOnMessageThrowsException_andTerminal_thenTerminated() {
        final ErrorListenerMock listener = new ErrorListenerMock();
        listener.isTerminal = true;
        topic.addMessageListener(listener);

        topic.publish("item1");

        assertTrueEventually(() -> {
            assertEquals(1, listener.objects.size());
            assertTrue(topic.runnersMap.isEmpty());
        });

        topic.publish("item2");

        // we need to make sure we don't receive item 2 since the listener is terminated
        assertTrueFiveSeconds(() -> assertEquals(1, listener.objects.size()));
    }

    @Test
    public void whenOnMessageThrowsException_andNotTerminal_thenListenerDoesNotTerminate() {
        final ErrorListenerMock listener = new ErrorListenerMock();
        listener.isTerminal = false;
        topic.addMessageListener(listener);

        topic.publish("item1");

        assertTrueEventually(() -> {
            assertEquals(1, listener.objects.size());
            assertFalse(topic.runnersMap.isEmpty());
        });

        topic.publish("item2");

        assertTrueEventually(() -> {
            assertEquals(2, listener.objects.size());
            assertFalse(topic.runnersMap.isEmpty());
        });
    }

    public class ErrorListenerMock extends ReliableMessageListenerMock {

        private volatile boolean throwErrorOnIsTerminal = false;

        @Override
        public void onMessage(Message<String> message) {
            super.onMessage(message);
            throw new ExpectedRuntimeException();
        }

        @Override
        public boolean isTerminal(Throwable failure) {
            if (throwErrorOnIsTerminal) {
                throw new ExpectedRuntimeException();
            }
            return super.isTerminal(failure);
        }
    }
}
