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

package uk.org.hecaton.jet.impl.execution;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.internal.nio.BufferObjectDataOutput;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReceiverTaskletTest {

    private ReceiverTasklet t;
    private MockOutboundCollector collector;
    private InternalSerializationService serService;
    private final Queue<byte[]> queue = new ArrayDeque<>();

    @Before
    public void before() {
        collector = new MockOutboundCollector(2);
        serService = new DefaultSerializationServiceBuilder().build();
        t = new ReceiverTasklet(collector, serService, 3, 100, mock(LoggingService.class),
                new Address(), 0, "", null, "");
        t.initIncomingQueue(queue);
    }

    @Test
    public void when_receiveTwoObjects_then_emitThem() throws IOException {
        pushObjects(1, 2);
        t.call();
        assertEquals(asList(1, 2), collector.getBuffer());
    }

    private void pushObjects(Object... objs) throws IOException {
        final BufferObjectDataOutput out = serService.createObjectDataOutput();
        // packet header
        out.writeLong(0);
        out.writeInt(0);
        out.writeInt(0);
        // the packet
        out.writeInt(objs.length);
        for (Object obj : objs) {
            out.writeObject(obj);
            out.writeInt(Math.abs(obj.hashCode())); // partition id
        }
        queue.add(out.toByteArray());
    }
}
