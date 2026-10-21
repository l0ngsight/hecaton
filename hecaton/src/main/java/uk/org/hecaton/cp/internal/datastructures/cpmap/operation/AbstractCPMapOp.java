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
package uk.org.hecaton.cp.internal.datastructures.cpmap.operation;

import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.internal.RaftOp;
import uk.org.hecaton.cp.internal.datastructures.cpmap.CPMapContainer;
import uk.org.hecaton.cp.internal.datastructures.cpmap.CPMapDataSerializerHook;
import uk.org.hecaton.cp.internal.datastructures.cpmap.CPMapService;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Base class of the operations on one key of a {@link uk.org.hecaton.cp.CPMap}.
 */
public abstract class AbstractCPMapOp extends RaftOp implements IdentifiedDataSerializable {

    protected String name;
    protected Data key;

    AbstractCPMapOp() {
    }

    AbstractCPMapOp(String name, Data key) {
        this.name = name;
        this.key = key;
    }

    CPMapContainer getContainer(CPGroupId groupId) {
        CPMapService service = getService();
        return service.getAtomicValue(groupId, name);
    }

    @Override
    public final String getServiceName() {
        return CPMapService.SERVICE_NAME;
    }

    @Override
    public final int getFactoryId() {
        return CPMapDataSerializerHook.F_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        IOUtil.writeData(out, key);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        key = IOUtil.readData(in);
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name);
    }
}
