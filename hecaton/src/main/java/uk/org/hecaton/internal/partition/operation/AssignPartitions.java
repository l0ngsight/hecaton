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

package uk.org.hecaton.internal.partition.operation;

import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.internal.partition.MigrationCycleOperation;
import uk.org.hecaton.internal.partition.PartitionRuntimeState;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.internal.partition.impl.PartitionDataSerializerHook;
import uk.org.hecaton.spi.exception.TargetNotMemberException;
import uk.org.hecaton.spi.impl.operationservice.ExceptionAction;

/** Sent from non-master nodes to the master to initialize the partition assignment. */
public class AssignPartitions extends AbstractPartitionOperation implements MigrationCycleOperation {

    private PartitionRuntimeState partitionState;

    @Override
    public void run() {
        InternalPartitionServiceImpl service = getService();
        partitionState = service.firstArrangement();
    }

    @Override
    public Object getResponse() {
        return partitionState;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.ASSIGN_PARTITIONS;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException
                || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }
}
