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

import uk.org.hecaton.client.impl.ClientEngine;
import uk.org.hecaton.client.impl.ClientEngineImpl;
import uk.org.hecaton.core.OperationTimeoutException;
import uk.org.hecaton.internal.locksupport.LockDataSerializerHook;
import uk.org.hecaton.internal.locksupport.LockStoreImpl;
import uk.org.hecaton.internal.locksupport.LockWaitNotifyKey;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.BlockingOperation;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.WaitNotifyKey;

public class LockOperation extends AbstractLockOperation implements BlockingOperation, BackupAwareOperation, MutatingOperation {

    public LockOperation() {
    }

    public LockOperation(ObjectNamespace namespace, Data key, long threadId, long leaseTime, long timeout) {
        super(namespace, key, threadId, leaseTime, timeout);
    }

    public LockOperation(ObjectNamespace namespace, Data key, long threadId, long leaseTime, long timeout, long referenceId,
                         boolean isClient) {
        super(namespace, key, threadId, leaseTime, timeout);
        this.isClient = isClient;
        setReferenceCallId(referenceId);
    }

    @Override
    public void run() throws Exception {
        interceptLockOperation();
        if (isClient) {
            ClientEngine clientEngine = getNodeEngine().getService(ClientEngineImpl.SERVICE_NAME);
            clientEngine.onClientAcquiredResource(getCallerUuid());
        }
        final boolean lockResult = getLockStore().lock(key, getCallerUuid(), threadId, getReferenceCallId(), leaseTime);
        response = lockResult;

        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            if (lockResult) {
                logger.finest("Acquired lock " + namespace.getObjectName()
                        + " for " + getCallerAddress() + " - " + getCallerUuid() + ", thread ID: " + threadId);
            } else {
                logger.finest("Could not acquire lock %s as owned by %s", namespace.getObjectName(),
                        getLockStore().getOwnerInfo(key));
            }
        }
    }

    @Override
    public Operation getBackupOperation() {
        LockBackupOperation operation = new LockBackupOperation(namespace, key, threadId, leaseTime, getCallerUuid(), isClient);
        operation.setReferenceCallId(getReferenceCallId());
        return operation;
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public final WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(namespace, key);
    }

    @Override
    public final boolean shouldWait() {
        LockStoreImpl lockStore = getLockStore();
        return getWaitTimeout() != 0 && !lockStore.canAcquireLock(key, getCallerUuid(), threadId);
    }

    @Override
    public final void onWaitExpire() {
        Object response;
        long timeout = getWaitTimeout();
        if (timeout < 0 || timeout == Long.MAX_VALUE) {
            response = new OperationTimeoutException();
        } else {
            response = Boolean.FALSE;
        }
        sendResponse(response);
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.LOCK;
    }
}
