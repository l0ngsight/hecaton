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
import uk.org.hecaton.internal.namespace.impl.NodeEngineThreadLocalContext;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.ringbuffer.StaleSequenceException;
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

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReadOneOperationTest extends HecatonTestSupport {

    private HecatonInstance hz;
    private NodeEngineImpl nodeEngine;
    private SerializationService serializationService;
    private Ringbuffer<Object> ringbuffer;
    private RingbufferContainer ringbufferContainer;
    private RingbufferService ringbufferService;


    @Before
    public void setup() {
        RingbufferConfig rbConfig = new RingbufferConfig("foo").setCapacity(10).setTimeToLiveSeconds(10);

        Config config = new Config().addRingBufferConfig(rbConfig);

        hz = createHecatonInstance(config);
        nodeEngine = getNodeEngineImpl(hz);
        serializationService = getSerializationService(hz);
        final String name = rbConfig.getName();
        ringbuffer = hz.getRingbuffer(name);

        ringbufferService = getNodeEngineImpl(hz).getService(RingbufferService.SERVICE_NAME);
        ringbufferContainer = ringbufferService.getOrCreateContainer(
                ringbufferService.getRingbufferPartitionId(name),
                RingbufferService.getRingbufferNamespace(name),
                rbConfig);
    }

    private Data toData(Object item) {
        return serializationService.toData(item);
    }

    @Test
    public void whenAtTail() throws Exception {
        NodeEngineThreadLocalContext.declareNodeEngineReference(nodeEngine);
        ringbuffer.add("tail");

        ReadOneOperation op = getReadOneOperation(ringbuffer.tailSequence());

        // since there is an item, we don't need to wait
        boolean shouldWait = op.shouldWait();
        assertFalse(shouldWait);

        op.run();

        assertEquals(toData("tail"), op.getResponse());
    }

    @Test
    public void whenOneAfterTail() {
        ringbuffer.add("tail");

        ReadOneOperation op = getReadOneOperation(ringbuffer.tailSequence() + 1);

        // since there is no item, we should wait
        boolean shouldWait = op.shouldWait();
        assertTrue(shouldWait);
    }

    @Test
    public void whenTooFarAfterTail() {
        ringbuffer.add("tail");

        ReadOneOperation op = getReadOneOperation(ringbuffer.tailSequence() + 2);

        // since there is an item, we don't need to wait
        op.shouldWait();
        assertThrows(IllegalArgumentException.class, op::beforeRun);
    }

    @Test
    public void whenOneAfterTailAndBufferEmpty() {
        ReadOneOperation op = getReadOneOperation(ringbuffer.tailSequence() + 1);

        // since there is an item, we don't need to wait
        boolean shouldWait = op.shouldWait();
        assertTrue(shouldWait);
    }

    @Test
    public void whenOnTailAndBufferEmpty() {
        ReadOneOperation op = getReadOneOperation(ringbuffer.tailSequence());

        // since there is an item, we don't need to wait
        op.shouldWait();
        assertThrows(StaleSequenceException.class, op::beforeRun);
    }

    @Test
    public void whenBeforeTail() throws Exception {
        NodeEngineThreadLocalContext.declareNodeEngineReference(nodeEngine);
        ringbuffer.add("item1");
        ringbuffer.add("item2");
        ringbuffer.add("item3");

        ReadOneOperation op = getReadOneOperation(ringbuffer.tailSequence() - 1);

        // since there is an item, we don't need to wait
        boolean shouldWait = op.shouldWait();
        assertFalse(shouldWait);

        op.run();

        assertEquals(toData("item2"), op.getResponse());
    }

    @Test
    public void whenAtHead() throws Exception {
        NodeEngineThreadLocalContext.declareNodeEngineReference(nodeEngine);
        ringbuffer.add("item1");
        ringbuffer.add("item2");
        ringbuffer.add("item3");

        ReadOneOperation op = getReadOneOperation(ringbuffer.headSequence());

        // since there is an item, we don't need to wait
        boolean shouldWait = op.shouldWait();
        assertFalse(shouldWait);

        op.run();

        assertEquals(toData("item1"), op.getResponse());
    }

    @Test
    public void whenBeforeHead() {
        ringbuffer.add("item1");
        ringbuffer.add("item2");
        ringbuffer.add("item3");

        long oldhead = ringbuffer.headSequence();
        ringbufferContainer.setHeadSequence(ringbufferContainer.tailSequence());

        ReadOneOperation op = getReadOneOperation(oldhead);

        op.shouldWait();
        assertThrows(StaleSequenceException.class, op::beforeRun);
    }

    private ReadOneOperation getReadOneOperation(long seq) {
        ReadOneOperation op = new ReadOneOperation(ringbuffer.getName(), seq);
        op.setPartitionId(ringbufferService.getRingbufferPartitionId(ringbuffer.getName()));
        op.setNodeEngine(nodeEngine);
        return op;
    }
}
