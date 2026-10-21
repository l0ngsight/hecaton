/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.cache.impl.wan;

import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.wan.WanEventCounters;
import uk.org.hecaton.wan.WanEventType;
import uk.org.hecaton.wan.impl.InternalWanEvent;
import uk.org.hecaton.wan.impl.WanDataSerializerHook;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/** A WAN event for a removed cache entry. */
public class WanCacheRemoveEvent implements InternalWanEvent<Data> {

    private String cacheName;
    private Data key;

    public WanCacheRemoveEvent() {
    }

    public WanCacheRemoveEvent(String cacheName, Data key) {
        this.cacheName = cacheName;
        this.key = key;
    }

    @Nonnull
    @Override
    public Data getKey() {
        return key;
    }

    @Nonnull
    @Override
    public Set<String> getClusterNames() {
        return Collections.emptySet();
    }

    @Override
    public int getBackupCount() {
        return 0;
    }

    @Override
    public long getCreationTime() {
        return 0;
    }

    @Override
    public void incrementEventCount(@Nonnull WanEventCounters counters) {
        counters.incrementRemove(cacheName);
    }

    @Nonnull
    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Nonnull
    @Override
    public String getObjectName() {
        return cacheName;
    }

    @Nonnull
    @Override
    public WanEventType getEventType() {
        return WanEventType.REMOVE;
    }

    @Override
    public Data getEventObject() {
        return key;
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.CACHE_REPLICATION_REMOVE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(cacheName);
        IOUtil.writeData(out, key);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        cacheName = in.readString();
        key = IOUtil.readData(in);
    }
}
