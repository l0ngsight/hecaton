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

import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ListenerTests extends ClientTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testSmartListenerRegister_whenNodeLeft() {
        int nodeCount = 5;
        for (int i = 0; i < nodeCount - 1; i++) {
            factory.newHecatonInstance();
        }
        final HecatonInstance node = factory.newHecatonInstance();

        HecatonInstance client = factory.newHecatonClient();
        IMap<Object, Object> map = client.getMap("test");


        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(() -> node.getLifecycleService().terminate(), 500, TimeUnit.MILLISECONDS);

        EntryAdapter listener = new EntryAdapter();

        LinkedList<UUID> registrationIds = new LinkedList<>();
        while (client.getCluster().getMembers().size() == nodeCount) {
            registrationIds.add(map.addEntryListener(listener, false));
        }

        for (UUID registrationId : registrationIds) {
            assertTrue(map.removeEntryListener(registrationId));
        }
        executorService.shutdown();

    }

    @Test
    public void testSmartListenerRegister_whenNodeJoined() {
        int nodeCount = 5;
        for (int i = 0; i < nodeCount - 1; i++) {
            factory.newHecatonInstance();
        }

        HecatonInstance client = factory.newHecatonClient();
        IMap<Object, Object> map = client.getMap("test");

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(() -> {
            factory.newHecatonInstance();
        }, 500, TimeUnit.MILLISECONDS);

        EntryAdapter listener = new EntryAdapter();

        LinkedList<UUID> registrationIds = new LinkedList<>();

        HecatonClientInstanceImpl clientInstance = getHecatonClientInstanceImpl(client);
        while (clientInstance.getConnectionManager().getActiveConnections().size() < nodeCount) {
            registrationIds.add(map.addEntryListener(listener, false));
        }

        for (UUID registrationId : registrationIds) {
            assertTrue(map.removeEntryListener(registrationId));
        }
        executorService.shutdown();

    }

    @Test
    public void testRemoveListenerOnClosedClient() {
        factory.newHecatonInstance();
        HecatonInstance client = factory.newHecatonClient();
        IMap<Object, Object> map = client.getMap("test");
        client.shutdown();
        assertTrue(map.removeEntryListener(UuidUtil.newUnsecureUUID()));
    }
}
