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
package uk.org.hecaton.wan.impl.batch;

import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.wan.impl.InternalWanEvent;
import uk.org.hecaton.wan.impl.WanDataSerializerHook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The events of some namespaces of a partition's WAN queue, replicated to a new replica of the
 * partition (migration, backup sync).
 */
public class WanPartitionEvents implements IdentifiedDataSerializable {

    private Collection<ServiceNamespace> namespaces;
    private List<InternalWanEvent<Object>> events;

    public WanPartitionEvents() {
    }

    WanPartitionEvents(Collection<ServiceNamespace> namespaces, List<InternalWanEvent<Object>> events) {
        this.namespaces = namespaces;
        this.events = events;
    }

    Collection<ServiceNamespace> namespaces() {
        return namespaces;
    }

    List<InternalWanEvent<Object>> events() {
        return events;
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.WAN_PARTITION_EVENTS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(namespaces.size());
        for (ServiceNamespace namespace : namespaces) {
            out.writeObject(namespace);
        }
        out.writeInt(events.size());
        for (InternalWanEvent<Object> event : events) {
            out.writeObject(event);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int namespaceCount = in.readInt();
        namespaces = new ArrayList<>(namespaceCount);
        for (int i = 0; i < namespaceCount; i++) {
            namespaces.add(in.readObject());
        }
        int eventCount = in.readInt();
        events = new ArrayList<>(eventCount);
        for (int i = 0; i < eventCount; i++) {
            events.add(in.readObject());
        }
    }
}
