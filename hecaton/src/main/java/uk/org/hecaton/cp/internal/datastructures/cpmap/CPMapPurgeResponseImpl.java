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
package uk.org.hecaton.cp.internal.datastructures.cpmap;

import uk.org.hecaton.cp.CPMapPurgeResponse;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Result of a {@link uk.org.hecaton.cp.CPDataStructureManagementService#purgeCPMap purge}.
 * {@link #oldestRemainingEntryTimestamp()} is {@code -1} when no entries remain.
 */
public class CPMapPurgeResponseImpl implements CPMapPurgeResponse, IdentifiedDataSerializable {

    private int purgedCount;
    private long oldestRemainingEntryTimestamp;

    public CPMapPurgeResponseImpl() {
    }

    public CPMapPurgeResponseImpl(int purgedCount, long oldestRemainingEntryTimestamp) {
        this.purgedCount = purgedCount;
        this.oldestRemainingEntryTimestamp = oldestRemainingEntryTimestamp;
    }

    @Override
    public int purgedCount() {
        return purgedCount;
    }

    @Override
    public long oldestRemainingEntryTimestamp() {
        return oldestRemainingEntryTimestamp;
    }

    @Override
    public int getFactoryId() {
        return CPMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CPMapDataSerializerHook.PURGE_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(purgedCount);
        out.writeLong(oldestRemainingEntryTimestamp);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        purgedCount = in.readInt();
        oldestRemainingEntryTimestamp = in.readLong();
    }

    @Override
    public String toString() {
        return "CPMapPurgeResponse{purgedCount=" + purgedCount
                + ", oldestRemainingEntryTimestamp=" + oldestRemainingEntryTimestamp + '}';
    }
}
