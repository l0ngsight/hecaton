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
import uk.org.hecaton.cp.internal.datastructures.cpmap.CPMapDataSerializerHook;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Operation for {@link uk.org.hecaton.cp.CPMap#remove(Object)} and {@link uk.org.hecaton.cp.CPMap#delete(Object)}.
 */
public class RemoveOp extends AbstractCPMapOp {

    private boolean returnValue;

    public RemoveOp() {
    }

    public RemoveOp(String name, Data key, boolean returnValue) {
        super(name, key);
        this.returnValue = returnValue;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        Data previous = getContainer(groupId).remove(key);
        return returnValue ? previous : null;
    }

    @Override
    public int getClassId() {
        return CPMapDataSerializerHook.REMOVE_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeBoolean(returnValue);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        returnValue = in.readBoolean();
    }
}
