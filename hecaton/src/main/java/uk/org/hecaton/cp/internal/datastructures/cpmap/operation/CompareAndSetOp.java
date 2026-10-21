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
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Operation for {@link uk.org.hecaton.cp.CPMap#compareAndSet}.
 */
public class CompareAndSetOp extends AbstractCPMapOp {

    private Data expectedValue;
    private Data newValue;
    private long timestamp;

    public CompareAndSetOp() {
    }

    public CompareAndSetOp(String name, Data key, Data expectedValue, Data newValue) {
        super(name, key);
        this.expectedValue = expectedValue;
        this.newValue = newValue;
        this.timestamp = Clock.currentTimeMillis();
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        return getContainer(groupId).compareAndSet(key, expectedValue, newValue, timestamp);
    }

    @Override
    public int getClassId() {
        return CPMapDataSerializerHook.COMPARE_AND_SET_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        IOUtil.writeData(out, expectedValue);
        IOUtil.writeData(out, newValue);
        out.writeLong(timestamp);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        expectedValue = IOUtil.readData(in);
        newValue = IOUtil.readData(in);
        timestamp = in.readLong();
    }
}
