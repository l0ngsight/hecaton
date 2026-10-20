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

import uk.org.hecaton.cache.impl.AbstractCacheService;
import uk.org.hecaton.cache.impl.CacheDataSerializerHook;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.cache.impl.PreJoinCacheConfig;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationservice.AbstractNamedOperation;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;
import uk.org.hecaton.spi.impl.operationservice.SelfResponseOperation;

import java.io.IOException;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;

/**
 * Gets a cache configuration or creates one, if a matching cache config is found in this member's config.
 *
 * @see AddCacheConfigOperation
 */
public class CacheGetConfigOperation extends AbstractNamedOperation implements IdentifiedDataSerializable, ReadonlyOperation,
                                                                               SelfResponseOperation {

    private transient volatile Object response;
    private transient InternalCompletableFuture createOnAllMembersFuture;
    private String simpleName;

    public CacheGetConfigOperation() {
    }

    public CacheGetConfigOperation(String name, String simpleName) {
        super(name);
        this.simpleName = simpleName;
    }

    @Override
    public void run()
            throws Exception {
        AbstractCacheService service = getService();
        CacheConfig cacheConfig = service.getCacheConfig(name);
        if (cacheConfig == null) {
            cacheConfig = service.findCacheConfig(simpleName);
            if (cacheConfig != null) {
                cacheConfig.setManagerPrefix(name.substring(0, name.lastIndexOf(simpleName)));
                CacheConfig existingCacheConfig = service.putCacheConfigIfAbsent(cacheConfig);
                if (existingCacheConfig != null) {
                    cacheConfig = existingCacheConfig;
                } else {
                    // a new cache config was added on the local member, all members should become aware of it
                    createOnAllMembersFuture = service.createCacheConfigOnAllMembersAsync(PreJoinCacheConfig.of(cacheConfig));
                }
            }
        }
        response = cacheConfig;
        if (createOnAllMembersFuture != null) {
            createOnAllMembersFuture.whenCompleteAsync((asyncResponse, t) -> {
                if (t == null) {
                    CacheGetConfigOperation.this.sendResponse(response);
                } else {
                    CacheGetConfigOperation.this.sendResponse(t);
                }
            }, CALLER_RUNS);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeString(simpleName);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        simpleName = in.readString();
    }

    @Override
    public boolean returnsResponse() {
        return createOnAllMembersFuture == null;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.GET_CONFIG;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
