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

package uk.org.hecaton.ringbuffer.impl.operations;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.ringbuffer.impl.RingbufferContainer;
import uk.org.hecaton.ringbuffer.impl.RingbufferService;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.ringbuffer.impl.operations.GenericOperation.OPERATION_CAPACITY;
import static uk.org.hecaton.ringbuffer.impl.operations.GenericOperation.OPERATION_HEAD;
import static uk.org.hecaton.ringbuffer.impl.operations.GenericOperation.OPERATION_REMAINING_CAPACITY;
import static uk.org.hecaton.ringbuffer.impl.operations.GenericOperation.OPERATION_SIZE;
import static uk.org.hecaton.ringbuffer.impl.operations.GenericOperation.OPERATION_TAIL;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GenericOperationTest extends HecatonTestSupport {

    private static final int CAPACITY = 10;

    private NodeEngineImpl nodeEngine;
    private Ringbuffer<Object> ringbuffer;
    private RingbufferContainer ringbufferContainer;
    private SerializationService serializationService;
    private RingbufferService ringbufferService;

    @Before
    public void setup() {
        RingbufferConfig rbConfig = new RingbufferConfig("foo")
                .setCapacity(CAPACITY)
                .setTimeToLiveSeconds(10);

        Config config = new Config().addRingBufferConfig(rbConfig);

        HecatonInstance hz = createHecatonInstance(config);
        nodeEngine = getNodeEngineImpl(hz);
        serializationService = nodeEngine.getSerializationService();
        final String name = rbConfig.getName();
        ringbuffer = hz.getRingbuffer(name);

        ringbufferService = getNodeEngineImpl(hz).getService(RingbufferService.SERVICE_NAME);
        ringbufferContainer = ringbufferService.getOrCreateContainer(
                ringbufferService.getRingbufferPartitionId(name),
                RingbufferService.getRingbufferNamespace(name),
                rbConfig);
    }

    @Test
    public void size() throws Exception {
        ringbuffer.add("a");
        ringbuffer.add("b");

        GenericOperation op = getGenericOperation(OPERATION_SIZE);

        op.run();
        Long result = op.getResponse();
        assertEquals(Long.valueOf(ringbufferContainer.size()), result);
    }

    @Test
    public void capacity() throws Exception {
        ringbuffer.add("a");
        ringbuffer.add("b");

        GenericOperation op = getGenericOperation(OPERATION_CAPACITY);

        op.run();
        Long result = op.getResponse();
        assertEquals(Long.valueOf(CAPACITY), result);
    }

    @Test
    public void remainingCapacity() throws Exception {
        ringbuffer.add("a");
        ringbuffer.add("b");

        GenericOperation op = getGenericOperation(OPERATION_REMAINING_CAPACITY);

        op.run();
        Long result = op.getResponse();
        assertEquals(Long.valueOf(CAPACITY - 2), result);
    }

    @Test
    public void tail() throws Exception {
        ringbuffer.add("a");
        ringbuffer.add("b");

        GenericOperation op = getGenericOperation(OPERATION_TAIL);

        op.run();
        Long result = op.getResponse();
        assertEquals(Long.valueOf(ringbufferContainer.tailSequence()), result);
    }

    @Test
    public void head() throws Exception {
        for (int k = 0; k < CAPACITY * 2; k++) {
            ringbuffer.add("a");
        }

        GenericOperation op = getGenericOperation(OPERATION_HEAD);

        op.run();
        Long result = op.getResponse();
        assertEquals(Long.valueOf(ringbufferContainer.headSequence()), result);
    }

    private GenericOperation getGenericOperation(byte operation) {
        GenericOperation op = new GenericOperation(ringbuffer.getName(), operation);
        op.setPartitionId(ringbufferService.getRingbufferPartitionId(ringbuffer.getName()));
        op.setNodeEngine(nodeEngine);
        return op;
    }

    public void serialize() {
        GenericOperation op = new GenericOperation(ringbuffer.getName(), OPERATION_HEAD);
        Data data = serializationService.toData(op);
        GenericOperation found = assertInstanceOf(GenericOperation.class, serializationService.toObject(data));

        assertEquals(op.operation, found.operation);
    }
}
