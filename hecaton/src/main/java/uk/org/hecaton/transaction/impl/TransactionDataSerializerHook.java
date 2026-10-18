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

package uk.org.hecaton.transaction.impl;

import uk.org.hecaton.internal.serialization.DataSerializerHook;
import uk.org.hecaton.internal.serialization.impl.FactoryIdHelper;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;
import uk.org.hecaton.transaction.impl.operations.BroadcastTxRollbackOperation;
import uk.org.hecaton.transaction.impl.operations.CreateAllowedDuringPassiveStateTxBackupLogOperation;
import uk.org.hecaton.transaction.impl.operations.CreateTxBackupLogOperation;
import uk.org.hecaton.transaction.impl.operations.PurgeAllowedDuringPassiveStateTxBackupLogOperation;
import uk.org.hecaton.transaction.impl.operations.PurgeTxBackupLogOperation;
import uk.org.hecaton.transaction.impl.operations.ReplicateAllowedDuringPassiveStateTxBackupLogOperation;
import uk.org.hecaton.transaction.impl.operations.ReplicateTxBackupLogOperation;
import uk.org.hecaton.transaction.impl.operations.RollbackAllowedDuringPassiveStateTxBackupLogOperation;
import uk.org.hecaton.transaction.impl.operations.RollbackTxBackupLogOperation;
import uk.org.hecaton.transaction.impl.xa.XATransactionDTO;
import uk.org.hecaton.transaction.impl.xa.operations.ClearRemoteTransactionBackupOperation;
import uk.org.hecaton.transaction.impl.xa.operations.ClearRemoteTransactionOperation;
import uk.org.hecaton.transaction.impl.xa.operations.CollectRemoteTransactionsOperation;
import uk.org.hecaton.transaction.impl.xa.operations.FinalizeRemoteTransactionBackupOperation;
import uk.org.hecaton.transaction.impl.xa.operations.FinalizeRemoteTransactionOperation;
import uk.org.hecaton.transaction.impl.xa.operations.PutRemoteTransactionBackupOperation;
import uk.org.hecaton.transaction.impl.xa.operations.PutRemoteTransactionOperation;
import uk.org.hecaton.transaction.impl.xa.operations.XaReplicationOperation;

import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.TRANSACTION_DS_FACTORY;
import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.TRANSACTION_DS_FACTORY_ID;

@SuppressWarnings("ClassDataAbstractionCoupling")
public final class TransactionDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(TRANSACTION_DS_FACTORY, TRANSACTION_DS_FACTORY_ID);

    public static final int CREATE_TX_BACKUP_LOG = 0;
    public static final int BROADCAST_TX_ROLLBACK = 1;
    public static final int PURGE_TX_BACKUP_LOG = 2;
    public static final int REPLICATE_TX_BACKUP_LOG = 3;
    public static final int ROLLBACK_TX_BACKUP_LOG = 4;
    public static final int CREATE_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG = 5;
    public static final int PURGE_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG = 6;
    public static final int REPLICATE_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG = 7;
    public static final int ROLLBACK_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG = 8;
    public static final int CLEAR_REMOTE_TX_BACKUP = 9;
    public static final int CLEAR_REMOTE_TX = 10;
    public static final int COLLECT_REMOTE_TX = 11;
    public static final int COLLECT_REMOTE_TX_FACTORY = 12;
    public static final int FINALIZE_REMOTE_TX_BACKUP = 13;
    public static final int FINALIZE_REMOTE_TX = 14;
    public static final int PUT_REMOTE_TX_BACKUP = 15;
    public static final int PUT_REMOTE_TX = 16;
    public static final int XA_REPLICATION = 17;
    public static final int XA_TRANSACTION_DTO = 18;


    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    @SuppressWarnings("CyclomaticComplexity")
    public DataSerializableFactory createFactory() {
        return typeId -> switch (typeId) {
            case CREATE_TX_BACKUP_LOG -> new CreateTxBackupLogOperation();
            case BROADCAST_TX_ROLLBACK -> new BroadcastTxRollbackOperation();
            case PURGE_TX_BACKUP_LOG -> new PurgeTxBackupLogOperation();
            case REPLICATE_TX_BACKUP_LOG -> new ReplicateTxBackupLogOperation();
            case ROLLBACK_TX_BACKUP_LOG -> new RollbackTxBackupLogOperation();
            case CREATE_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG -> new CreateAllowedDuringPassiveStateTxBackupLogOperation();
            case PURGE_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG -> new PurgeAllowedDuringPassiveStateTxBackupLogOperation();
            case REPLICATE_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG
                    -> new ReplicateAllowedDuringPassiveStateTxBackupLogOperation();
            case ROLLBACK_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG
                    -> new RollbackAllowedDuringPassiveStateTxBackupLogOperation();
            case CLEAR_REMOTE_TX_BACKUP -> new ClearRemoteTransactionBackupOperation();
            case CLEAR_REMOTE_TX -> new ClearRemoteTransactionOperation();
            case COLLECT_REMOTE_TX -> new CollectRemoteTransactionsOperation();
            case FINALIZE_REMOTE_TX_BACKUP -> new FinalizeRemoteTransactionBackupOperation();
            case FINALIZE_REMOTE_TX -> new FinalizeRemoteTransactionOperation();
            case PUT_REMOTE_TX_BACKUP -> new PutRemoteTransactionBackupOperation();
            case PUT_REMOTE_TX -> new PutRemoteTransactionOperation();
            case XA_REPLICATION -> new XaReplicationOperation();
            case XA_TRANSACTION_DTO -> new XATransactionDTO();
            default -> null;
        };
    }
}
