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
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.eventservice.impl.EventServiceImpl;
import uk.org.hecaton.spi.impl.eventservice.impl.EventServiceSegment;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.UUID;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapEntryListenerOnReconnectTest extends AbstractListenersOnReconnectTest {

    private ReplicatedMap<String, String> replicatedMap;

    @Parameterized.Parameters(name = "{index}: routingMode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(RoutingMode.SINGLE_MEMBER, RoutingMode.ALL_MEMBERS);
    }

    @Override
    String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    protected UUID addListener() {
        replicatedMap = client.getReplicatedMap(randomString());
        final EntryAdapter<String, String> listener = new EntryAdapter<>() {
            @Override
            public void onEntryEvent(EntryEvent<String, String> event) {
                onEvent(event.getKey());
            }
        };
        return replicatedMap.addEntryListener(listener);
    }

    @Override
    public void produceEvent(String event) {
        replicatedMap.put(event, randomString());
    }

    @Override
    public boolean removeListener(UUID registrationId) {
        return replicatedMap.removeEntryListener(registrationId);
    }

    @Override
    protected void validateRegistrationsOnMembers(final TestHecatonFactory factory, int expected) {
        assertTrueEventually(() -> {
            boolean found = false;
            for (HecatonInstance instance : factory.getAllHecatonInstances()) {
                NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
                EventServiceImpl eventService = (EventServiceImpl) nodeEngineImpl.getEventService();
                EventServiceSegment serviceSegment = eventService.getSegment(getServiceName(), false);
                if (serviceSegment != null && serviceSegment.getRegistrationIdMap().size() == expected) {
                    found = true;
                }
            }
            assertTrue(found);
        });
    }
}
