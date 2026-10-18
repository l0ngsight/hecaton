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

package uk.org.hecaton.multimap.impl.operations;

import uk.org.hecaton.internal.locksupport.LockWaitNotifyKey;
import uk.org.hecaton.core.OperationTimeoutException;
import uk.org.hecaton.multimap.impl.MultiMapContainer;
import uk.org.hecaton.multimap.impl.MultiMapDataSerializerHook;
import uk.org.hecaton.multimap.impl.MultiMapService;
import uk.org.hecaton.multimap.impl.MultiMapValue;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.BlockingOperation;
import uk.org.hecaton.internal.services.DistributedObjectNamespace;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;
import uk.org.hecaton.spi.impl.operationservice.WaitNotifyKey;

public class CountOperation extends AbstractKeyBasedMultiMapOperation implements BlockingOperation, ReadonlyOperation {

    public CountOperation() {
    }

    public CountOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        ((MultiMapService) getService()).getLocalMultiMapStatsImpl(name).incrementOtherOperations();
        MultiMapValue multiMapValue = container.getMultiMapValueOrNull(dataKey);
        response = multiMapValue == null ? 0 : multiMapValue.getCollection(false).size();
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.COUNT;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, name), dataKey);
    }

    @Override
    public boolean shouldWait() {
        MultiMapContainer container = getOrCreateContainer();
        if (container.isTransactionallyLocked(dataKey)) {
            return !container.canAcquireLock(dataKey, getCallerUuid(), threadId);
        }
        return false;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(new OperationTimeoutException("Cannot read transactionally locked entry!"));
    }
}
