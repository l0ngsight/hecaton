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

package uk.org.hecaton.map.impl.operation;

import uk.org.hecaton.core.OperationTimeoutException;
import uk.org.hecaton.internal.locksupport.LockWaitNotifyKey;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.impl.MapDataSerializerHook;
import uk.org.hecaton.map.impl.operation.steps.ContainsKeyOpSteps;
import uk.org.hecaton.map.impl.operation.steps.engine.Step;
import uk.org.hecaton.map.impl.operation.steps.engine.State;
import uk.org.hecaton.spi.impl.operationservice.BlockingOperation;
import uk.org.hecaton.spi.impl.operationservice.WaitNotifyKey;

public class ContainsKeyOperation
        extends ReadonlyKeyBasedMapOperation implements BlockingOperation {

    private transient boolean containsKey;

    public ContainsKeyOperation() {
    }

    public ContainsKeyOperation(String name, Data dataKey) {
        this.name = name;
        this.dataKey = dataKey;
    }

    @Override
    protected void runInternal() {
        containsKey = recordStore.containsKey(dataKey, getCallerAddress());
    }

    @Override
    public void applyState(State state) {
        super.applyState(state);
        containsKey = state.getOldValue() != null;
    }

    @Override
    public Step getStartingStep() {
        return ContainsKeyOpSteps.READ;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.CONTAINS_KEY;
    }

    @Override
    public Object getResponse() {
        return containsKey;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(getServiceNamespace(), dataKey);
    }

    @Override
    public boolean shouldWait() {
        if (recordStore.isTransactionallyLocked(dataKey)) {
            return !recordStore.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
        }
        return false;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(new OperationTimeoutException("Cannot read transactionally locked entry!"));
    }
}
