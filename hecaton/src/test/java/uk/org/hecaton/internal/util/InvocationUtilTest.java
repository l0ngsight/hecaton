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

package uk.org.hecaton.internal.util;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.partition.impl.InternalPartitionImpl;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationservice.AbstractLocalOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationResponseHandler;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.util.InvocationUtil.executeLocallyWithRetry;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvocationUtilTest extends HecatonTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(InvocationUtil.class);
    }


    @Test(expected = IllegalArgumentException.class)
    public void executeLocallyWithRetryFailsWhenOperationHandlerIsSet() {
        final Operation op = new Operation() {
        };
        op.setOperationResponseHandler(new OperationResponseHandler() {
            @Override
            public void sendResponse(Operation op, Object response) {

            }
        });
        executeLocallyWithRetry(null, op);
    }

    @Test(expected = IllegalArgumentException.class)
    public void executeLocallyWithRetryFailsWhenOperationDoesNotReturnResponse() {
        final Operation op = new Operation() {
            @Override
            public boolean returnsResponse() {
                return false;
            }
        };
        executeLocallyWithRetry(null, op);
    }

    @Test(expected = IllegalArgumentException.class)
    public void executeLocallyWithRetryFailsWhenOperationValidatesTarget() {
        final Operation op = new Operation() {
            @Override
            public boolean validatesTarget() {
                return true;
            }
        };
        executeLocallyWithRetry(null, op);
    }

    @Test
    public void executeLocallyRetriesWhenPartitionIsMigrating() throws InterruptedException {
        final HecatonInstance instance = createHecatonInstance(smallInstanceConfig());
        final NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        final InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
        final int randomPartitionId = (int) (Math.random() * partitionService.getPartitionCount());
        final InternalPartitionImpl partition = (InternalPartitionImpl) partitionService.getPartition(randomPartitionId);
        partition.setMigrating();

        final String operationResponse = "operationResponse";
        final Operation operation = new LocalOperation(operationResponse)
                .setPartitionId(randomPartitionId);
        final LocalRetryableExecution execution = executeLocallyWithRetry(nodeEngineImpl, operation);

        spawn((Runnable) () -> {
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {

            }
            partition.resetMigrating();
        });

        assertTrue(execution.awaitCompletion(1, TimeUnit.MINUTES));
        assertEquals(operationResponse, execution.getResponse());
    }

    public class LocalOperation extends AbstractLocalOperation {
        private final Object operationResponse;
        private Object response;

        public LocalOperation(Object operationResponse) {
            this.operationResponse = operationResponse;
        }

        @Override
        public void run() throws Exception {
            response = operationResponse;
        }

        @Override
        public Object getResponse() {
            return response;
        }

        @Override
        public boolean validatesTarget() {
            return false;
        }
    }
}
