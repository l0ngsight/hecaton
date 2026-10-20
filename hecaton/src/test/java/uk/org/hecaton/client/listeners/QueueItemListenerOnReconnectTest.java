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

package uk.org.hecaton.client.listeners;

import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.collection.impl.queue.QueueService;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ItemEvent;
import uk.org.hecaton.collection.ItemListener;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.UUID;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueItemListenerOnReconnectTest extends AbstractListenersOnReconnectTest {

    private IQueue<String> iQueue;

    @Parameterized.Parameters(name = "{index}: routingMode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(RoutingMode.SINGLE_MEMBER, RoutingMode.ALL_MEMBERS);
    }

    @Override
    String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    protected UUID addListener() {
        iQueue = client.getQueue(randomString());

        ItemListener<String> listener = new ItemListener<>() {
            @Override
            public void itemAdded(ItemEvent<String> item) {
                onEvent(item.getItem());
            }

            @Override
            public void itemRemoved(ItemEvent item) {

            }
        };
        return iQueue.addItemListener(listener, true);
    }

    @Override
    public void produceEvent(String event) {
        iQueue.add(event);
    }

    @Override
    public boolean removeListener(UUID registrationId) {
        return iQueue.removeItemListener(registrationId);
    }
}
