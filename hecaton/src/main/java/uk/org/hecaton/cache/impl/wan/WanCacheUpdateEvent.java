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

import uk.org.hecaton.cache.CacheEntryView;
import uk.org.hecaton.cache.impl.ICacheService;
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

/**
 * A WAN event for an added or updated cache entry: the entry and the name of the merge policy the
 * target cluster applies it with.
 */
public class WanCacheUpdateEvent implements InternalWanEvent<CacheEntryView<Object, Object>> {

    private String cacheName;
    private String mergePolicyClassName;
    private WanCacheEntryView<Object, Object> entryView;

    public WanCacheUpdateEvent() {
    }

    public WanCacheUpdateEvent(String cacheName, String mergePolicyClassName, WanCacheEntryView<Object, Object> entryView) {
        this.cacheName = cacheName;
        this.mergePolicyClassName = mergePolicyClassName;
        this.entryView = entryView;
    }

    public String getMergePolicyClassName() {
        return mergePolicyClassName;
    }

    public WanCacheEntryView<Object, Object> getEntryView() {
        return entryView;
    }

    @Nonnull
    @Override
    public Data getKey() {
        return entryView.getDataKey();
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
        counters.incrementUpdate(cacheName);
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
        return WanEventType.ADD_OR_UPDATE;
    }

    @Override
    public CacheEntryView<Object, Object> getEventObject() {
        return entryView;
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.CACHE_REPLICATION_UPDATE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(cacheName);
        out.writeString(mergePolicyClassName);
        out.writeObject(entryView);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        cacheName = in.readString();
        mergePolicyClassName = in.readString();
        entryView = in.readObject();
    }
}
