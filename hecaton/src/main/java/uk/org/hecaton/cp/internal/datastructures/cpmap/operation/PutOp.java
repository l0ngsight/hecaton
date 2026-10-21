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
import uk.org.hecaton.cp.internal.datastructures.cpmap.CPMapContainer;
import uk.org.hecaton.cp.internal.datastructures.cpmap.CPMapDataSerializerHook;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Operation for {@link uk.org.hecaton.cp.CPMap#put}, {@link uk.org.hecaton.cp.CPMap#set} and
 * {@link uk.org.hecaton.cp.CPMap#putIfAbsent}.
 */
public class PutOp extends AbstractCPMapOp {

    /**
     * What a {@link PutOp} does and returns.
     */
    public enum Mode {
        /** Put and return the previous value. */
        PUT,
        /** Put and return nothing. */
        SET,
        /** Put only if absent; return the current value. */
        PUT_IF_ABSENT
    }

    private Data value;
    private Mode mode;
    private long timestamp;

    public PutOp() {
    }

    public PutOp(String name, Data key, Data value, Mode mode) {
        super(name, key);
        this.value = value;
        this.mode = mode;
        this.timestamp = Clock.currentTimeMillis();
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        CPMapContainer container = getContainer(groupId);
        if (mode == Mode.PUT_IF_ABSENT) {
            Data current = container.get(key);
            if (current != null) {
                return current;
            }
        }
        Data previous = container.put(key, value, timestamp);
        return mode == Mode.PUT ? previous : null;
    }

    @Override
    public int getClassId() {
        return CPMapDataSerializerHook.PUT_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        IOUtil.writeData(out, value);
        out.writeByte(mode.ordinal());
        out.writeLong(timestamp);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        value = IOUtil.readData(in);
        mode = Mode.values()[in.readByte()];
        timestamp = in.readLong();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", mode=").append(mode);
    }
}
