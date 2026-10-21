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
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.wan.impl.CallerProvenance;

import java.io.IOException;
import java.util.UUID;

/**
 * Operation implementation for cache remove functionality.
 *
 * @see uk.org.hecaton.cache.impl.ICacheRecordStore#remove(Data, UUID, UUID, int)
 * @see uk.org.hecaton.cache.impl.ICacheRecordStore#remove(Data, Object, UUID, UUID, int)
 */
public class CacheRemoveOperation extends MutatingCacheOperation {

    // if same
    private Data oldValue;
    /** Whether the removal was received by WAN replication: it is not replicated again. */
    private boolean wanOriginated;

    public CacheRemoveOperation() {
    }

    public CacheRemoveOperation(String cacheNameWithPrefix, Data key, Data oldValue, int completionId) {
        this(cacheNameWithPrefix, key, oldValue, completionId, false);
    }

    public CacheRemoveOperation(String cacheNameWithPrefix, Data key, Data oldValue, int completionId,
                                boolean wanOriginated) {
        super(cacheNameWithPrefix, key, completionId);
        this.oldValue = oldValue;
        this.wanOriginated = wanOriginated;
    }

    @Override
    protected boolean isWanOriginated() {
        return wanOriginated;
    }

    @Override
    public void run()
            throws Exception {
        if (wanOriginated) {
            response = recordStore.remove(key, getCallerUuid(), null, completionId, CallerProvenance.WAN);
        } else if (oldValue == null) {
            response = recordStore.remove(key, getCallerUuid(), null, completionId);
        } else {
            response = recordStore.remove(key, oldValue, getCallerUuid(), null, completionId);
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (Boolean.TRUE.equals(response)) {
            publishWanRemove(key);
        }
        super.afterRun();
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheRemoveBackupOperation(name, key, wanOriginated);
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.REMOVE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, oldValue);
        out.writeBoolean(wanOriginated);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        oldValue = IOUtil.readData(in);
        wanOriginated = in.readBoolean();
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
