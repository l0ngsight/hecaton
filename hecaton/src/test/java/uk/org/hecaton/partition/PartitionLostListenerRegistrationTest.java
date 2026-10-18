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

package uk.org.hecaton.partition;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.UUID;

import static uk.org.hecaton.internal.partition.InternalPartitionService.PARTITION_LOST_EVENT_TOPIC;
import static uk.org.hecaton.internal.partition.InternalPartitionService.SERVICE_NAME;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionLostListenerRegistrationTest extends HecatonTestSupport {

    @Test(expected = NullPointerException.class)
    public void test_addPartitionLostListener_whenNullListener() {
        HecatonInstance hz = createHecatonInstance();
        PartitionService partitionService = hz.getPartitionService();

        partitionService.addPartitionLostListener(null);
    }

    @Test
    public void test_addPartitionLostListener_whenListenerRegisteredProgrammatically() {
        final HecatonInstance instance = createHecatonInstance();

        final UUID id = instance.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        assertNotNull(id);

        // Expected = 4 -> 1 added + 1 from {@link uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 4);
    }

    @Test
    public void test_partitionLostListener_whenListenerRegisteredViaConfiguration() {
        Config config = new Config();
        config.addListenerConfig(new ListenerConfig(mock(PartitionLostListener.class)));

        HecatonInstance instance = createHecatonInstance(config);
        // Expected = 4 -> 1 added + 1 from {@link uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 4);
    }

    @Test
    public void test_addPartitionLostListener_whenListenerRegisteredTwice() {
        HecatonInstance instance = createHecatonInstance();
        PartitionService partitionService = instance.getPartitionService();

        PartitionLostListener listener = mock(PartitionLostListener.class);

        UUID id1 = partitionService.addPartitionLostListener(listener);
        UUID id2 = partitionService.addPartitionLostListener(listener);

        assertNotEquals(id1, id2);
        // Expected = 5 -> 2 added + 1 from {@link uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 5);
    }

    @Test
    public void test_removePartitionLostListener_whenRegisteredListenerRemovedSuccessfully() {
        HecatonInstance instance = createHecatonInstance();
        PartitionService partitionService = instance.getPartitionService();

        PartitionLostListener listener = mock(PartitionLostListener.class);

        UUID id1 = partitionService.addPartitionLostListener(listener);
        boolean result = partitionService.removePartitionLostListener(id1);

        assertTrue(result);
        // Expected = 1 -> see {@link uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers
        assertRegistrationsSizeEventually(instance, 3);
    }

    @Test
    public void test_removePartitionLostListener_whenNonExistingRegistrationIdRemoved() {
        HecatonInstance instance = createHecatonInstance();
        PartitionService partitionService = instance.getPartitionService();

        boolean result = partitionService.removePartitionLostListener(UuidUtil.newUnsecureUUID());
        assertFalse(result);
    }

    @Test(expected = NullPointerException.class)
    public void test_removePartitionLostListener_whenNullRegistrationIdRemoved() {
        HecatonInstance instance = createHecatonInstance();
        PartitionService partitionService = instance.getPartitionService();

        partitionService.removePartitionLostListener(null);
    }

    private static void assertRegistrationsSizeEventually(final HecatonInstance instance, final int expectedSize) {
        assertTrueEventually(() -> assertTrueEventually(() -> {
            EventService eventService = getNode(instance).getNodeEngine().getEventService();
            Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME,
                    PARTITION_LOST_EVENT_TOPIC);
            assertEquals(expectedSize, registrations.size());
        }));
    }
}
