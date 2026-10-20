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

package uk.org.hecaton.transaction.impl.xa.operations;

import uk.org.hecaton.internal.serialization.impl.SerializationUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.transaction.impl.TransactionDataSerializerHook;
import uk.org.hecaton.transaction.impl.xa.XAService;
import uk.org.hecaton.transaction.impl.xa.XATransaction;
import uk.org.hecaton.transaction.impl.xa.XATransactionDTO;

import java.io.IOException;
import java.util.List;

public class XaReplicationOperation extends Operation implements IdentifiedDataSerializable {

    private List<XATransactionDTO> migrationData;

    public XaReplicationOperation() {
    }

    public XaReplicationOperation(List<XATransactionDTO> migrationData, int partitionId, int replicaIndex) {
        setPartitionId(partitionId);
        setReplicaIndex(replicaIndex);
        this.migrationData = migrationData;
    }

    @Override
    public void run() throws Exception {
        XAService xaService = getService();
        NodeEngine nodeEngine = getNodeEngine();
        for (XATransactionDTO transactionDTO : migrationData) {
            XATransaction transaction = new XATransaction(
                    nodeEngine,
                    transactionDTO.getRecords(),
                    transactionDTO.getTxnId(),
                    transactionDTO.getXid(),
                    transactionDTO.getOwnerUuid(),
                    transactionDTO.getTimeoutMilis(),
                    transactionDTO.getStartTime());
            xaService.putTransaction(transaction);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        SerializationUtil.writeList(migrationData, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        migrationData = SerializationUtil.readList(in);
    }

    @Override
    public int getFactoryId() {
        return TransactionDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return TransactionDataSerializerHook.XA_REPLICATION;
    }
}
