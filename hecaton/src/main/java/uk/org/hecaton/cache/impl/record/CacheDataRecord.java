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

package uk.org.hecaton.cache.impl.record;

import uk.org.hecaton.cache.impl.CacheDataSerializerHook;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;

import java.io.IOException;

/**
 * Implementation of {@link uk.org.hecaton.cache.impl.record.CacheRecord} where value has an internal serialized format.
 */
public class CacheDataRecord extends AbstractCacheRecord<Data, Data> {

    private Data value;
    private Data expiryPolicy;

    // Deserialization constructor
    public CacheDataRecord() {
    }

    public CacheDataRecord(Data value, long creationTime, long expiryTime) {
        super(creationTime, expiryTime);
        this.value = value;
    }

    @Override
    public Data getValue() {
        return value;
    }

    @Override
    public void setValue(Data value) {
        this.value = value;
    }

    @Override
    public void setExpiryPolicy(Data expiryPolicy) {
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    public Data getExpiryPolicy() {
        return expiryPolicy;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        IOUtil.writeData(out, value);
        IOUtil.writeData(out, expiryPolicy);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        value = IOUtil.readData(in);
        expiryPolicy = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.CACHE_DATA_RECORD;
    }
}
