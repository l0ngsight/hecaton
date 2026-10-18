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

import uk.org.hecaton.cache.CacheNotExistsException;
import uk.org.hecaton.cache.impl.CacheDataSerializerHook;
import uk.org.hecaton.cache.impl.ICacheRecordStore;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.BackupOperation;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.internal.services.ServiceNamespaceAware;
import uk.org.hecaton.spi.impl.operationservice.AbstractNamedOperation;

import java.io.IOException;
import java.util.Set;

import static uk.org.hecaton.internal.util.SetUtil.createHashSet;

/**
 * Backup operation of {@link uk.org.hecaton.cache.impl.operation.CacheRemoveAllOperation}.
 * <p>It simply clears the records of all removed keys.</p>
 */
public class CacheRemoveAllBackupOperation
        extends AbstractNamedOperation
        implements BackupOperation, ServiceNamespaceAware, IdentifiedDataSerializable {

    private Set<Data> keys;

    private transient ICacheRecordStore cache;

    public CacheRemoveAllBackupOperation() {
    }

    public CacheRemoveAllBackupOperation(String name, Set<Data> keys) {
        super(name);
        this.keys = keys;
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.REMOVE_ALL_BACKUP;
    }

    @Override
    public void beforeRun()
            throws Exception {
        ICacheService service = getService();
        try {
            cache = service.getOrCreateRecordStore(name, getPartitionId());
        } catch (CacheNotExistsException e) {
            getLogger().finest("Error while getting a cache", e);
        }
    }

    @Override
    public void run()
            throws Exception {
        if (cache == null) {
            return;
        }
        if (keys != null) {
            for (Data key : keys) {
                cache.removeRecord(key);
            }
        }
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
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeBoolean(keys != null);
        if (keys != null) {
            out.writeInt(keys.size());
            for (Data key : keys) {
                IOUtil.writeData(out, key);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        boolean isKeysNotNull = in.readBoolean();
        if (isKeysNotNull) {
            int size = in.readInt();
            keys = createHashSet(size);
            for (int i = 0; i < size; i++) {
                Data key = IOUtil.readData(in);
                keys.add(key);
            }
        }
    }

}
