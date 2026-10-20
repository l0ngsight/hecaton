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

package uk.org.hecaton.cache.impl.operation;

import uk.org.hecaton.cache.impl.CacheDataSerializerHook;
import uk.org.hecaton.cache.impl.CacheMergeResponse;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.impl.SerializationUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static uk.org.hecaton.wan.impl.CallerProvenance.NOT_WAN;

/**
 * Contains multiple merging entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class CacheMergeOperation extends CacheOperation implements BackupAwareOperation {

    private List<CacheMergeTypes<Object, Object>> mergingEntries;
    private SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> mergePolicy;

    private transient boolean hasBackups;
    private transient List backupPairs;
    private transient BitSet backupNonReplicatedKeys;

    public CacheMergeOperation() {
    }

    public CacheMergeOperation(String name, List<CacheMergeTypes<Object, Object>> mergingEntries,
                               SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> mergePolicy) {
        super(name);
        this.mergingEntries = mergingEntries;
        this.mergePolicy = mergePolicy;
    }

    @Override
    protected void beforeRunInternal() {
        hasBackups = getSyncBackupCount() + getAsyncBackupCount() > 0;
        if (hasBackups) {
            backupPairs = new ArrayList(mergingEntries.size());
            backupNonReplicatedKeys = new BitSet(mergingEntries.size());
        }
    }

    @Override
    public void run() {
        for (CacheMergeTypes<Object, Object> mergingEntry : mergingEntries) {
            merge(mergingEntry);
        }
    }

    private void merge(CacheMergeTypes<Object, Object> mergingEntry) {
        Data dataKey = (Data) mergingEntry.getRawKey();

        CacheMergeResponse response = recordStore.merge(mergingEntry, mergePolicy, NOT_WAN);
        if (backupPairs != null && response.getResult().isMergeApplied()) {
            if (response.getResult() == CacheMergeResponse.MergeResult.RECORDS_ARE_EQUAL) {
                backupNonReplicatedKeys.set(backupPairs.size() / 2);
            }
            backupPairs.add(dataKey);
            backupPairs.add(response.getRecord());
        }
        if (recordStore.isWanReplicationEnabled()) {
            if (response.getResult().isMergeApplied()) {
                // Don't WAN replicate merge events where values don't change
                if (response.getResult() != CacheMergeResponse.MergeResult.RECORDS_ARE_EQUAL) {
                    publishWanUpdate(dataKey, response.getRecord());
                }
            } else {
                publishWanRemove(dataKey);
            }
        }
    }

    @Override
    public Object getResponse() {
        return hasBackups && !backupPairs.isEmpty();
    }

    @Override
    public boolean shouldBackup() {
        return hasBackups && !backupPairs.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutAllBackupOperation(name, backupPairs, backupNonReplicatedKeys);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(mergingEntries.size());
        for (CacheMergeTypes<Object, Object> mergingEntry : mergingEntries) {
            out.writeObject(mergingEntry);
        }
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        mergingEntries = SerializationUtil.readList(in);
        mergePolicy = NamespaceUtil.callWithNamespace(in::readObject, name, CacheService::lookupNamespace);
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.MERGE;
    }
}
