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

package uk.org.hecaton.transaction.impl.operations;

import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.internal.util.UUIDSerializationUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.operationservice.ExceptionAction;
import uk.org.hecaton.spi.exception.TargetNotMemberException;
import uk.org.hecaton.transaction.impl.TransactionManagerServiceImpl;

import java.io.IOException;
import java.util.UUID;

import static uk.org.hecaton.spi.impl.operationservice.ExceptionAction.THROW_EXCEPTION;
import static uk.org.hecaton.transaction.impl.TransactionDataSerializerHook.ROLLBACK_TX_BACKUP_LOG;

public class RollbackTxBackupLogOperation extends AbstractTxOperation {

    private UUID txnId;

    public RollbackTxBackupLogOperation() {
    }

    public RollbackTxBackupLogOperation(UUID txnId) {
        this.txnId = txnId;
    }

    @Override
    public void run() throws Exception {
        TransactionManagerServiceImpl txManagerService = getService();
        txManagerService.rollbackBackupLog(txnId);
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public int getClassId() {
        return ROLLBACK_TX_BACKUP_LOG;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        UUIDSerializationUtil.writeUUID(out, txnId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        txnId = UUIDSerializationUtil.readUUID(in);
    }
}
