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
import uk.org.hecaton.ringbuffer.OverflowPolicy;
import uk.org.hecaton.ringbuffer.Ringbuffer;
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

import static uk.org.hecaton.internal.namespace.impl.NodeEngineThreadLocalContext.declareNodeEngineReference;
import static uk.org.hecaton.ringbuffer.OverflowPolicy.FAIL;
import static uk.org.hecaton.ringbuffer.OverflowPolicy.OVERWRITE;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AddOperationsTest extends HecatonTestSupport {

    private NodeEngineImpl nodeEngine;
    private SerializationService serializationService;
    private Ringbuffer<Object> ringbuffer;
    private RingbufferService ringbufferService;

    @Before
    public void setup() {
        RingbufferConfig rbConfig = new RingbufferConfig("foo").setCapacity(10).setTimeToLiveSeconds(10);

        Config config = new Config().addRingBufferConfig(rbConfig);

        HecatonInstance hz = createHecatonInstance(config);
        nodeEngine = getNodeEngineImpl(hz);
        ringbufferService = nodeEngine.getService(RingbufferService.SERVICE_NAME);
        serializationService = getSerializationService(hz);
        ringbuffer = hz.getRingbuffer(rbConfig.getName());
    }

    @Test
    public void whenFailOverflowPolicy_andNoRemainingCapacity_thenNoBackup() throws Exception {
        declareNodeEngineReference(nodeEngine);
        for (int k = 0; k < ringbuffer.capacity(); k++) {
            ringbuffer.add("item");
        }

        Data item = serializationService.toData("newItem");

        AddOperation addOperation = getAddOperation(item, FAIL);
        addOperation.run();

        assertFalse(addOperation.shouldBackup());
        assertFalse(addOperation.shouldNotify());
        assertEquals(Long.valueOf(-1L), addOperation.getResponse());
    }

    @Test
    public void whenFailOverflowPolicy_andRemainingCapacity_thenBackup() throws Exception {
        declareNodeEngineReference(nodeEngine);
        for (int k = 0; k < ringbuffer.capacity() - 1; k++) {
            ringbuffer.add("item");
        }

        Data item = serializationService.toData("newItem");

        AddOperation addOperation = getAddOperation(item, FAIL);
        addOperation.run();

        assertTrue(addOperation.shouldBackup());
        assertTrue(addOperation.shouldNotify());
        assertEquals(Long.valueOf(ringbuffer.tailSequence()), addOperation.getResponse());
    }

    @Test
    public void whenOverwritePolicy_andNoRemainingCapacity_thenBackup() throws Exception {
        declareNodeEngineReference(nodeEngine);
        for (int k = 0; k < ringbuffer.capacity(); k++) {
            ringbuffer.add("item");
        }

        Data item = serializationService.toData("newItem");

        AddOperation addOperation = getAddOperation(item, OVERWRITE);
        addOperation.run();

        assertTrue(addOperation.shouldBackup());
        assertTrue(addOperation.shouldNotify());
        assertEquals(Long.valueOf(ringbuffer.tailSequence()), addOperation.getResponse());
    }

    @Test
    public void whenOverwritePolicy_andRemainingCapacity_thenBackup() throws Exception {
        declareNodeEngineReference(nodeEngine);
        for (int k = 0; k < ringbuffer.capacity() - 1; k++) {
            ringbuffer.add("item");
        }

        Data item = serializationService.toData("newItem");

        AddOperation addOperation = getAddOperation(item, OVERWRITE);
        addOperation.run();

        assertTrue(addOperation.shouldNotify());
        assertTrue(addOperation.shouldBackup());
        assertEquals(Long.valueOf(ringbuffer.tailSequence()), addOperation.getResponse());
    }

    private AddOperation getAddOperation(Data item, OverflowPolicy policy) {
        AddOperation op = new AddOperation(ringbuffer.getName(), item, policy);
        op.setPartitionId(ringbufferService.getRingbufferPartitionId(ringbuffer.getName()));
        op.setNodeEngine(nodeEngine);
        return op;
    }
}
