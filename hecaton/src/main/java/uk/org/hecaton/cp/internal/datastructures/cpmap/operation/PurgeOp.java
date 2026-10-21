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
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Operation for {@link uk.org.hecaton.cp.CPDataStructureManagementService#purgeCPMap}: removes the
 * entries last written more than {@code ageMillis} before the operation was created.
 */
public class PurgeOp extends RaftOp implements IdentifiedDataSerializable {

    private String name;
    private long cutoff;

    public PurgeOp() {
    }

    public PurgeOp(String name, long ageMillis) {
        this.name = name;
        this.cutoff = Clock.currentTimeMillis() - ageMillis;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        CPMapService service = getService();
        CPMapContainer container = service.getExistingContainer(groupId, name);
        if (container == null) {
            throw new NoSuchElementException("CPMap '" + name + "' does not exist in CP group " + groupId.getName());
        }
        return container.purge(cutoff);
    }

    @Override
    public String getServiceName() {
        return CPMapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CPMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CPMapDataSerializerHook.PURGE_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeLong(cutoff);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        cutoff = in.readLong();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name).append(", cutoff=").append(cutoff);
    }
}
