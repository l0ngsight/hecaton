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

package uk.org.hecaton.ringbuffer.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static uk.org.hecaton.internal.partition.MigrationEndpoint.DESTINATION;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingbufferServiceTest extends HecatonTestSupport {

    private RingbufferService service;
    private HecatonInstance hz;

    @Before
    public void setup() {
        hz = createHecatonInstance();
        service = getNodeEngineImpl(hz).getService(RingbufferService.SERVICE_NAME);
    }

    @Test
    public void rollbackMigration() {
        Ringbuffer ringbuffer = hz.getRingbuffer("foo");
        int partitionId = getPartitionId(hz, ringbuffer.getName());
        PartitionMigrationEvent partitionEvent = new PartitionMigrationEvent(DESTINATION, partitionId, -1, 0, UUID.randomUUID());

        service.rollbackMigration(partitionEvent);

        assertEquals(0, service.getContainers().size());
    }

    @Test
    public void reset() {
        final String foo = "foo";
        final String bar = "bar";
        service.getOrCreateContainer(
                service.getRingbufferPartitionId(foo),
                RingbufferService.getRingbufferNamespace(foo),
                service.getRingbufferConfig(foo));
        service.getOrCreateContainer(
                service.getRingbufferPartitionId(bar),
                RingbufferService.getRingbufferNamespace(bar),
                service.getRingbufferConfig(bar));

        service.reset();

        assertEquals(0, service.getContainers().size());
    }
}
