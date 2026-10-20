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

package uk.org.hecaton.map.impl.operation;

import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.namespace.impl.NodeEngineThreadLocalContext;
import uk.org.hecaton.map.MapInterceptor;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapDataSerializerHook;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.AbstractNamedOperation;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;

public class AddInterceptorOperation extends AbstractNamedOperation
        implements MutatingOperation {

    private String id;
    private MapInterceptor mapInterceptor;

    public AddInterceptorOperation() {
    }

    public AddInterceptorOperation(String mapName,
                                   String id,
                                   MapInterceptor mapInterceptor) {
        super(mapName);
        this.id = id;
        this.mapInterceptor = mapInterceptor;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void run() {
        getMapContainer().getInterceptorRegistry().register(id, mapInterceptor);
    }

    private MapContainer getMapContainer() {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapContainer(name);
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        id = in.readString();
        NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
        mapInterceptor = NamespaceUtil.callWithNamespace(engine, MapService.lookupNamespace(engine, name), in::readObject);
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(id);
        out.writeObject(mapInterceptor);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(name);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.ADD_INTERCEPTOR;
    }
}
