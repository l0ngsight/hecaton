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

import uk.org.hecaton.cache.impl.CacheClearResponse;
import uk.org.hecaton.cache.impl.CacheDataSerializerHook;
import uk.org.hecaton.cache.impl.ICacheRecordStore;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.cache.impl.record.CacheRecord;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.internal.services.ServiceNamespaceAware;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.AbstractNamedOperation;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;

import javax.cache.CacheException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static uk.org.hecaton.internal.util.SetUtil.createHashSet;

/**
 * Loads all entries of the keys to partition record store {@link uk.org.hecaton.cache.impl.ICacheRecordStore}.
 * <p>{@link uk.org.hecaton.cache.impl.operation.CacheLoadAllOperationFactory} creates this operation.</p>
 * <p>Functionality: Filters out the partition keys and calls
 * {@link uk.org.hecaton.cache.impl.ICacheRecordStore#loadAll(java.util.Set keys, boolean replaceExistingValues)}.</p>
 */
public class CacheLoadAllOperation
        extends AbstractNamedOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable, BackupAwareOperation,
        MutatingOperation, ServiceNamespaceAware {

    private Set<Data> keys;
    private boolean replaceExistingValues;
    private boolean shouldBackup;

    private transient List backupPairs;
    private transient ICacheRecordStore cache;

    private Object response;

    public CacheLoadAllOperation(String name, Set<Data> keys, boolean replaceExistingValues) {
        super(name);
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
    }

    public CacheLoadAllOperation() {
    }

    @Override
    public void run()
            throws Exception {
        int partitionId = getPartitionId();
        IPartitionService partitionService = getNodeEngine().getPartitionService();

        Set<Data> filteredKeys = null;
        if (keys != null) {
            filteredKeys = new HashSet<>();
            for (Data k : keys) {
                if (partitionService.getPartitionId(k) == partitionId) {
                    filteredKeys.add(k);
                }
            }
        }

        if (filteredKeys == null || filteredKeys.isEmpty()) {
            return;
        }

        try {
            ICacheService service = getService();
            cache = service.getOrCreateRecordStore(name, partitionId);
            Set<Data> keysLoaded = cache.loadAll(filteredKeys, replaceExistingValues);
            int loadedKeyCount = keysLoaded.size();
            if (loadedKeyCount > 0) {
                backupPairs = new ArrayList<>(loadedKeyCount * 2);
                for (Data key : keysLoaded) {
                    CacheRecord record = cache.getRecord(key);
                    // Loaded keys may have been evicted, then record will be null.
                    // So if the loaded key is evicted, don't send it to backup.
                    if (record != null) {
                        backupPairs.add(key);
                        backupPairs.add(record);
                    }
                }
                shouldBackup = !backupPairs.isEmpty();
            }
        } catch (CacheException e) {
            response = new CacheClearResponse(e);
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutAllBackupOperation(name, backupPairs);
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.LOAD_ALL;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public final int getSyncBackupCount() {
        return cache.getConfig().getBackupCount();
    }

    @Override
    public final int getAsyncBackupCount() {
        return cache.getConfig().getAsyncBackupCount();
    }

    @Override
    public ObjectNamespace getServiceNamespace() {
        ICacheRecordStore recordStore = cache;
        if (recordStore == null) {
            ICacheService service = getService();
            recordStore = service.getOrCreateRecordStore(name, getPartitionId());
        }
        return recordStore.getObjectNamespace();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(replaceExistingValues);
        out.writeBoolean(keys != null);
        if (keys != null) {
            out.writeInt(keys.size());
            for (Data key : keys) {
                IOUtil.writeData(out, key);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        replaceExistingValues = in.readBoolean();
        if (in.readBoolean()) {
            int size = in.readInt();
            keys = createHashSet(size);
            for (int i = 0; i < size; i++) {
                keys.add(IOUtil.readData(in));
            }
        }
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
