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
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationexecutor.impl.OperationExecutorImpl;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.test.HecatonTestSupport;

import java.io.IOException;

import static uk.org.hecaton.test.Accessors.getOperationService;
import static uk.org.hecaton.test.Accessors.getPartitionService;

abstract class Invocation_NestedAbstractTest extends HecatonTestSupport {

    static final int GENERIC_OPERATION = -1;

    public static boolean mappedToSameThread(OperationService operationService, int partitionId1, int partitionId2) {
        OperationServiceImpl operationServiceImpl = (OperationServiceImpl) operationService;
        OperationExecutorImpl executor = (OperationExecutorImpl) operationServiceImpl.getOperationExecutor();
        int thread1 = executor.toPartitionThreadIndex(partitionId1);
        int thread2 = executor.toPartitionThreadIndex(partitionId2);
        return thread1 == thread2;
    }

    public static class OuterOperation extends Operation {

        public Operation innerOperation;
        public Object result;

        @SuppressWarnings("unused")
        OuterOperation() {
        }

        OuterOperation(Operation innerOperation, int partitionId) {
            this.innerOperation = innerOperation;
            setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
            int partitionId = innerOperation.getPartitionId();
            OperationService operationService = getNodeEngine().getOperationService();
            InternalCompletableFuture<Object> f;
            if (partitionId >= 0) {
                f = operationService.invokeOnPartition(null, innerOperation, partitionId);
            } else {
                f = operationService.invokeOnTarget(null, innerOperation, getNodeEngine().getThisAddress());
            }

            result = f.join();
        }

        @Override
        public Object getResponse() {
            return result;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeObject(innerOperation);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            innerOperation = in.readObject();
        }
    }

    public static class InnerOperation extends Operation {
        public Object value;

        @SuppressWarnings("unused")
        InnerOperation() {
        }

        InnerOperation(Object value, int partitionId) {
            this.value = value;

            setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        public Object getResponse() {
            return value;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeObject(value);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            value = in.readObject();
        }
    }

    protected static int randomPartitionIdNotMappedToSameThreadAsGivenPartitionIdOnInstance(HecatonInstance hz,
                                                                                            int givenPartitionId) {
        int resultPartitionId;
        for (resultPartitionId = 0; resultPartitionId < hz.getPartitionService().getPartitions().size(); resultPartitionId++) {
            if (resultPartitionId == givenPartitionId) {
                continue;
            }
            if (!getPartitionService(hz).getPartition(resultPartitionId).isLocal()) {
                continue;
            }
            if (!mappedToSameThread(getOperationService(hz), givenPartitionId, resultPartitionId)) {
                break;
            }
        }
        return resultPartitionId;
    }

    protected static int randomPartitionIdMappedToSameThreadAsGivenPartitionIdOnInstance(int givenPartitionId,
                                                                                         HecatonInstance instance,
                                                                                         OperationService operationService) {
        int resultPartitionId = 0;
        for (; resultPartitionId < instance.getPartitionService().getPartitions().size(); resultPartitionId++) {
            if (resultPartitionId == givenPartitionId) {
                continue;
            }
            if (!getPartitionService(instance).getPartition(resultPartitionId).isLocal()) {
                continue;
            }
            if (mappedToSameThread(operationService, givenPartitionId, resultPartitionId)) {
                break;
            }
        }
        return resultPartitionId;
    }
}
