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
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.record.CacheRecord;
import uk.org.hecaton.internal.namespace.impl.NodeEngineThreadLocalContext;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.BackupOperation;

import javax.cache.processor.EntryProcessor;
import java.io.IOException;

import static uk.org.hecaton.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static uk.org.hecaton.internal.namespace.NamespaceUtil.callWithNamespace;

/**
 * Operation of the Cache Backup Entry Processor.
 * <p>{@link uk.org.hecaton.cache.BackupAwareEntryProcessor} is executed on the partition.
 * Executing this method applies a backup entry processor to the requested
 * {@link uk.org.hecaton.cache.impl.ICacheRecordStore} which provides the required
 * functionality to apply the backup using the given {@link javax.cache.processor.EntryProcessor}.</p>
 */
public class CacheBackupEntryProcessorOperation
        extends KeyBasedCacheOperation
        implements BackupOperation, IdentifiedDataSerializable {

    private EntryProcessor entryProcessor;
    private Object[] arguments;

    public CacheBackupEntryProcessorOperation() {
    }

    public CacheBackupEntryProcessorOperation(String cacheNameWithPrefix, Data key, EntryProcessor entryProcessor,
                                              Object... arguments) {
        super(cacheNameWithPrefix, key);
        this.entryProcessor = entryProcessor;
        this.arguments = arguments;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.BACKUP_ENTRY_PROCESSOR;
    }

    @Override
    public void run() throws Exception {
        if (recordStore != null) {
            recordStore.invoke(key, entryProcessor, arguments, IGNORE_COMPLETION);
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (recordStore == null) {
            return;
        }

        if (recordStore.isWanReplicationEnabled()) {
            CacheRecord record = recordStore.getRecord(key);
            if (record != null) {
                publishWanUpdate(key, record);
            } else {
                publishWanRemove(key);
            }
        }

        super.afterRun();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
        out.writeBoolean(arguments != null);
        if (arguments != null) {
            out.writeInt(arguments.length);
            for (Object arg : arguments) {
                out.writeObject(arg);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
        entryProcessor = callWithNamespace(engine,
                CacheService.lookupNamespace(engine, name), in::readObject);
        final boolean hasArguments = in.readBoolean();
        if (hasArguments) {
            final int size = in.readInt();
            arguments = new Object[size];
            for (int i = 0; i < size; i++) {
                arguments[i] = in.readObject();
            }
        }
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
