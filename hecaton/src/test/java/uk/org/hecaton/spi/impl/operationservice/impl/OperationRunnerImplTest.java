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

package uk.org.hecaton.spi.impl.operationservice.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.nio.Packet;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;
import uk.org.hecaton.spi.impl.operationservice.BlockingOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationResponseHandler;
import uk.org.hecaton.spi.impl.operationservice.WaitNotifyKey;
import uk.org.hecaton.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import uk.org.hecaton.test.ExpectedRuntimeException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicLong;

import static uk.org.hecaton.internal.util.counters.SwCounter.newSwCounter;
import static uk.org.hecaton.spi.impl.operationservice.OperationAccessor.setCallId;
import static uk.org.hecaton.spi.impl.operationservice.OperationAccessor.setCallTimeout;
import static uk.org.hecaton.test.Accessors.getClusterService;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationRunnerImplTest extends HecatonTestSupport {

    private HecatonInstance local;
    private HecatonInstance remote;
    private OperationRunnerImpl operationRunner;
    private OperationServiceImpl operationService;
    private ClusterService clusterService;
    private OperationResponseHandler responseHandler;

    @Before
    public void setup() {
        HecatonInstance[] cluster = createHecatonInstanceFactory(2).newInstances();
        local = cluster[0];
        remote = cluster[1];
        operationService = getOperationService(local);
        clusterService = getClusterService(local);
        operationRunner = new OperationRunnerImpl(operationService, getPartitionId(local), 0, newSwCounter(), null);
        responseHandler = mock(OperationResponseHandler.class);
    }

    @Test
    public void runTask() {
        final AtomicLong counter = new AtomicLong();
        operationRunner.run(counter::incrementAndGet);
        assertEquals(1, counter.get());
    }

    @Test
    public void runOperation() {
        final AtomicLong counter = new AtomicLong();
        final Object response = "someresponse";
        Operation op = new Operation() {
            @Override
            public void run() {
                counter.incrementAndGet();
            }

            @Override
            public Object getResponse() {
                return response;
            }
        };
        op.setPartitionId(operationRunner.getPartitionId());
        op.setOperationResponseHandler(responseHandler);

        operationRunner.run(op);
        assertEquals(1, counter.get());
        verify(responseHandler).sendResponse(op, response);
    }

    @Test
    public void runOperation_whenGeneric() {
        final AtomicLong counter = new AtomicLong();
        final Object response = "someresponse";

        Operation op = new Operation() {
            @Override
            public void run() {
                counter.incrementAndGet();
            }

            @Override
            public Object getResponse() {
                return response;
            }
        };
        op.setPartitionId(-1);
        op.setOperationResponseHandler(responseHandler);

        operationRunner.run(op);

        assertEquals(1, counter.get());
        verify(responseHandler).sendResponse(op, response);
    }

    @Test
    public void runOperation_whenWrongPartition_thenTaskNotExecuted() {
        final AtomicLong counter = new AtomicLong();

        Operation op = new Operation() {
            @Override
            public void run() throws Exception {
                counter.incrementAndGet();
            }
        };
        op.setPartitionId(operationRunner.getPartitionId() + 1);
        op.setOperationResponseHandler(responseHandler);

        operationRunner.run(op);

        assertEquals(0, counter.get());
        verify(responseHandler).sendResponse(same(op), any(IllegalStateException.class));
    }

    @Test
    public void runOperation_whenRunThrowsException() {
        Operation op = new Operation() {
            @Override
            public void run() throws Exception {
                throw new ExpectedRuntimeException();
            }
        };
        op.setOperationResponseHandler(responseHandler);
        op.setPartitionId(operationRunner.getPartitionId());

        operationRunner.run(op);

        verify(responseHandler).sendResponse(same(op), any(ExpectedRuntimeException.class));
    }

    @Test
    public void runOperation_whenWaitingNeeded() {
        final AtomicLong counter = new AtomicLong();

        DummyWaitingOperation op = new DummyWaitingOperation() {
            @Override
            public void run() {
                counter.incrementAndGet();
            }
        };
        op.setPartitionId(operationRunner.getPartitionId());
        op.setOperationResponseHandler(responseHandler);

        operationRunner.run(op);
        assertEquals(0, counter.get());
        // verify that the response handler was not called
        verifyNoMoreInteractions(responseHandler);
    }

    @Test
    public void runOperation_whenTimeout_thenOperationNotExecuted() {
        final AtomicLong counter = new AtomicLong();

        Operation op = new Operation() {
            @Override
            public void run() {
                counter.incrementAndGet();
            }
        };
        setCallId(op, 10);
        setCallTimeout(op, clusterService.getClusterClock().getClusterTime() - 1);
        op.setPartitionId(operationRunner.getPartitionId());
        op.setOperationResponseHandler(responseHandler);

        operationRunner.run(op);
        assertEquals(0, counter.get());
        verify(responseHandler).sendResponse(same(op), any(CallTimeoutResponse.class));
    }

    @Test
    public void runPacket() throws Exception {
        Operation op = new DummyOperation();
        setCallId(op, 1000 * 1000);

        Packet packet = toPacket(local, remote, op);
        operationRunner.run(packet);
    }

    @Test(expected = HecatonSerializationException.class)
    public void runPacket_whenBroken() throws Exception {
        Operation op = new DummyOperation();
        setCallId(op, 1000 * 1000);

        Packet packet = toPacket(local, remote, op);
        byte[] bytes = packet.toByteArray();
        for (int k = 0; k < bytes.length; k++) {
            bytes[k]++;
        }
        operationRunner.run(packet);
    }

    public abstract class DummyWaitingOperation extends Operation implements BlockingOperation {
        WaitNotifyKey waitNotifyKey = new WaitNotifyKey() {
            @Override
            public String getServiceName() {
                return "someservice";
            }

            @Override
            public String getObjectName() {
                return "someobject";
            }
        };

        @Override
        public WaitNotifyKey getWaitKey() {
            return waitNotifyKey;
        }

        @Override
        public boolean shouldWait() {
            return true;
        }

        @Override
        public void onWaitExpire() {
        }
    }
}
