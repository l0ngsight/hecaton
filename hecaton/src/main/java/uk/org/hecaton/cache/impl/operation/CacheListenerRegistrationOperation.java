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
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.namespace.impl.NodeEngineThreadLocalContext;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.AbstractNamedOperation;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import java.io.IOException;

/**
 * <p>Listener can be registered and unregistered by {@link uk.org.hecaton.cache.impl.CacheService}. This operator
 * delegates the request to the cache service.</p>
 */
public class CacheListenerRegistrationOperation
        extends AbstractNamedOperation
        implements IdentifiedDataSerializable {

    private CacheEntryListenerConfiguration cacheEntryListenerConfiguration;
    private boolean register;

    public CacheListenerRegistrationOperation() {
    }

    public CacheListenerRegistrationOperation(String name, CacheEntryListenerConfiguration cacheEntryListenerConfiguration,
                                              boolean register) {
        super(name);
        this.cacheEntryListenerConfiguration = cacheEntryListenerConfiguration;
        this.register = register;
    }

    @Override
    public void run()
            throws Exception {
        final AbstractCacheService service = getService();
        CacheConfig cacheConfig = service.getCacheConfig(name);
        if (register) {
            service.cacheEntryListenerRegistered(name, cacheEntryListenerConfiguration);
        } else if (cacheConfig != null) {
            service.cacheEntryListenerDeregistered(name, cacheEntryListenerConfiguration);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(cacheEntryListenerConfiguration);
        out.writeBoolean(register);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
        cacheEntryListenerConfiguration = NamespaceUtil.callWithNamespace(engine,
                CacheService.lookupNamespace(engine, name), in::readObject);
        register = in.readBoolean();
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.LISTENER_REGISTRATION;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

}
