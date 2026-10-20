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

import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.UUID;

import static uk.org.hecaton.client.config.RoutingMode.ALL_MEMBERS;
import static uk.org.hecaton.client.config.RoutingMode.SINGLE_MEMBER;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryListenerOnReconnectTest extends AbstractListenersOnReconnectTest {

    private IMap<String, String> iMap;

    @Parameterized.Parameters(name = "{index}: routingMode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(SINGLE_MEMBER, ALL_MEMBERS);
    }

    @Override
    String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    protected UUID addListener() {
        iMap = client.getMap(randomString());

        final EntryAdapter<String, String> listener = new EntryAdapter<>() {
            public void onEntryEvent(EntryEvent<String, String> event) {
                onEvent(event.getKey());
            }
        };
        return iMap.addEntryListener(listener, true);
    }

    @Override
    public void produceEvent(String event) {
        iMap.put(event, randomString());
    }

    @Override
    public boolean removeListener(UUID registrationId) {
        return iMap.removeEntryListener(registrationId);
    }
}
