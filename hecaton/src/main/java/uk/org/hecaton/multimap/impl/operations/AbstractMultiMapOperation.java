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

package uk.org.hecaton.multimap.impl.operations;

import uk.org.hecaton.config.MultiMapConfig;
import uk.org.hecaton.core.EntryEventType;
import uk.org.hecaton.multimap.impl.MultiMapContainer;
import uk.org.hecaton.multimap.impl.MultiMapDataSerializerHook;
import uk.org.hecaton.multimap.impl.MultiMapService;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.NamedOperation;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;
import uk.org.hecaton.internal.services.ServiceNamespaceAware;

import java.io.IOException;

import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

public abstract class AbstractMultiMapOperation extends Operation
        implements NamedOperation, PartitionAwareOperation, ServiceNamespaceAware, IdentifiedDataSerializable {

    protected String name;

    protected transient Object response;

    private transient MultiMapContainer container;

    protected AbstractMultiMapOperation() {
    }

    protected AbstractMultiMapOperation(String name) {
        this.name = name;
    }

    @Override
    public final Object getResponse() {
        return response;
    }

    @Override
    public final String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }

    public final void publishEvent(EntryEventType eventType, Data key, Object newValue, Object oldValue) {
        MultiMapService multiMapService = getService();
        multiMapService.publishEntryEvent(name, eventType, key, newValue, oldValue);
    }

    public final Object toObject(Object obj) {
        return getNodeEngine().toObject(obj);
    }

    public final Data toData(Object obj) {
        return getNodeEngine().toData(obj);
    }

    public final MultiMapContainer getOrCreateContainer() {
        if (container == null) {
            MultiMapService service = getService();
            container = service.getOrCreateCollectionContainer(getPartitionId(), name);
        }
        return container;
    }

    public final MultiMapContainer getOrCreateContainerWithoutAccess() {
        if (container == null) {
            MultiMapService service = getService();
            container = service.getOrCreateCollectionContainerWithoutAccess(getPartitionId(), name);
        }
        return container;
    }

    public final MultiMapConfig.ValueCollectionType getValueCollectionType(MultiMapContainer container) {
        checkNotNull(container, "Argument container should not be null");

        MultiMapConfig config = container.getConfig();
        return config.getValueCollectionType();
    }

    public final boolean isBinary() {
        return getOrCreateContainer().getConfig().isBinary();
    }

    public final int getSyncBackupCount() {
        return getOrCreateContainer().getConfig().getBackupCount();
    }

    public final int getAsyncBackupCount() {
        return getOrCreateContainer().getConfig().getAsyncBackupCount();
    }

    @Override
    public ObjectNamespace getServiceNamespace() {
        MultiMapContainer container = getOrCreateContainerWithoutAccess();
        return container.getObjectNamespace();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
    }

    @Override
    public int getFactoryId() {
        return MultiMapDataSerializerHook.F_ID;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(name);
    }
}
