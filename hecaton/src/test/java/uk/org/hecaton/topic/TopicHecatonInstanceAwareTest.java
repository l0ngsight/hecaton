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

package uk.org.hecaton.topic;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TopicHecatonInstanceAwareTest extends HecatonTestSupport {
    private CompletableFuture<HecatonInstance> hecatonInstanceFuture;

    @Before
    public void setUp() {
        hecatonInstanceFuture = new CompletableFuture<>();
    }

    @Test
    public void testInjected() {
        createHecatonInstance().getTopic(randomName()).addMessageListener(new MyMessageListener<>());

        assertTrueEventually(() -> assertNotNull(hecatonInstanceFuture.get()));
    }

    private class MyMessageListener<V> implements MessageListener<V>, HecatonInstanceAware {
        @Override
        public void onMessage(Message<V> message) {
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            if (hecatonInstance != null) {
                hecatonInstanceFuture.complete(hecatonInstance);
            }
        }
    }
}
