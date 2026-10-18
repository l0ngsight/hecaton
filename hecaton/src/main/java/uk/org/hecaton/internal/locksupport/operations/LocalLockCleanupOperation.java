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

import uk.org.hecaton.internal.locksupport.LockResource;
import uk.org.hecaton.internal.locksupport.LockStoreImpl;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.Notifier;
import uk.org.hecaton.spi.impl.operationservice.SplitBrainProtectionCheckAwareOperation;
import uk.org.hecaton.internal.partition.IPartition;
import uk.org.hecaton.internal.partition.IPartitionService;

import java.io.IOException;
import java.util.UUID;

/**
 * Locally executed operation which unlocks a lock if it is held by a
 * specific owner.
 */
public class LocalLockCleanupOperation extends UnlockOperation
        implements Notifier, BackupAwareOperation, SplitBrainProtectionCheckAwareOperation {

    private final UUID uuid;

    /**
     * This constructor should not be used to obtain an instance of this class; it exists to fulfill IdentifiedDataSerializable
     * coding conventions.
     */
    public LocalLockCleanupOperation() {
        uuid = UuidUtil.newUnsecureUUID();
    }

    public LocalLockCleanupOperation(ObjectNamespace namespace, Data key, UUID uuid) {
        super(namespace, key, -1, true);
        this.uuid = uuid;
    }

    @Override
    public void run() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        LockResource lock = lockStore.getLock(key);
        if (uuid.equals(lock.getOwner())) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest(
                        "Unlocking lock owned by UUID: " + uuid + ", thread ID: " + lock.getThreadId() + ", count: "
                                + lock.getLockCount());
            }
            response = lockStore.forceUnlock(key);
        }
    }

    @Override
    public boolean shouldBackup() {
        final NodeEngine nodeEngine = getNodeEngine();
        IPartitionService partitionService = nodeEngine.getPartitionService();
        IPartition partition = partitionService.getPartition(getPartitionId());
        return partition.isLocal() && Boolean.TRUE.equals(response);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException("LocalLockCleanupOperation is local only.");
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException("LocalLockCleanupOperation is local only.");
    }

    @Override
    public int getClassId() {
        throw new UnsupportedOperationException("LocalLockCleanupOperation is local only.");
    }

    @Override
    public boolean shouldCheckSplitBrainProtection() {
        return false;
    }
}
