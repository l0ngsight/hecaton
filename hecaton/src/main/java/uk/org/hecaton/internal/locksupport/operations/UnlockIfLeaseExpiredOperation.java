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

import uk.org.hecaton.internal.locksupport.LockDataSerializerHook;
import uk.org.hecaton.internal.locksupport.LockStoreImpl;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.SplitBrainProtectionCheckAwareOperation;
import uk.org.hecaton.internal.partition.IPartition;
import uk.org.hecaton.internal.partition.IPartitionService;

import java.io.IOException;

public final class UnlockIfLeaseExpiredOperation extends UnlockOperation implements SplitBrainProtectionCheckAwareOperation {

    private int version;

    public UnlockIfLeaseExpiredOperation() {
    }

    public UnlockIfLeaseExpiredOperation(ObjectNamespace namespace, Data key, int version) {
        super(namespace, key, -1, true);
        this.version = version;
    }

    @Override
    public void run() {
        LockStoreImpl lockStore = getLockStore();
        int lockVersion = lockStore.getVersion(key);
        ILogger logger = getLogger();
        if (version == lockVersion) {
            if (logger.isFinestEnabled()) {
                logger.finest("Releasing a lock owned by %s after lease timeout!", lockStore.getOwnerInfo(key));
            }
            forceUnlock();
        } else {
            if (logger.isFinestEnabled()) {
                logger.finest("Won't unlock since lock version is not matching expiration version: %s vs %s", lockVersion,
                        version);
            }
        }
    }

    /**
     * This operation runs on both primary and backup
     * If it is running on backup we should not send a backup operation
     */
    @Override
    public boolean shouldBackup() {
        NodeEngine nodeEngine = getNodeEngine();
        IPartitionService partitionService = nodeEngine.getPartitionService();
        Address thisAddress = nodeEngine.getThisAddress();
        IPartition partition = partitionService.getPartition(getPartitionId());
        if (!thisAddress.equals(partition.getOwnerOrNull())) {
            return false;
        }
        return super.shouldBackup();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(version);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        version = in.readInt();
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.UNLOCK_IF_LEASE_EXPIRED;
    }

    @Override
    public boolean shouldCheckSplitBrainProtection() {
        return false;
    }
}
