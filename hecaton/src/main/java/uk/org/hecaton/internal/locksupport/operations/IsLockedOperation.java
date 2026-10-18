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

package uk.org.hecaton.internal.locksupport.operations;

import uk.org.hecaton.internal.locksupport.LockStoreImpl;
import uk.org.hecaton.internal.locksupport.LockDataSerializerHook;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;

public class IsLockedOperation extends AbstractLockOperation implements ReadonlyOperation {

    public IsLockedOperation() {
    }

    public IsLockedOperation(ObjectNamespace namespace, Data key) {
        super(namespace, key, ANY_THREAD);
    }

    public IsLockedOperation(ObjectNamespace namespace, Data key, long threadId) {
        super(namespace, key, threadId);
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.IS_LOCKED;
    }

    @Override
    public void run() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        if (threadId == ANY_THREAD) {
            response = lockStore.isLocked(key);
        } else {
            response = lockStore.isLockedBy(key, getCallerUuid(), threadId);
        }
    }
}
