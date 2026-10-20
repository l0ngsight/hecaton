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

import uk.org.hecaton.cache.impl.record.CacheRecord;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;

import java.io.IOException;

/**
 * Operations running on a single key should extend this class.
 */
public abstract class KeyBasedCacheOperation extends CacheOperation {

    protected Data key;
    protected Object response;

    protected transient CacheRecord backupRecord;

    protected KeyBasedCacheOperation() {
    }

    protected KeyBasedCacheOperation(String name, Data key) {
        this(name, key, false);
    }

    protected KeyBasedCacheOperation(String name, Data key,
                                     boolean dontCreateCacheRecordStoreIfNotExist) {
        super(name, dontCreateCacheRecordStoreIfNotExist);
        this.key = key;
    }

    @Override
    public final Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, key);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        key = IOUtil.readData(in);
    }
}
