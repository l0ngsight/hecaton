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
package uk.org.hecaton.persistence.internal.cluster;

import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * What a member reports to the master during recovery.
 */
public final class RecoveryStatus implements DataSerializable {

    /** Loading has not been asked for. */
    public static final int NOT_LOADED = 0;
    /** Loading runs. */
    public static final int LOADING = 1;
    /** The data is loaded. */
    public static final int LOADED = 2;
    /** Loading failed. */
    public static final int LOAD_FAILED = 3;

    private boolean hasData;
    private long tableStamp;
    private long tableVersionSum;
    private int loadState;
    private String failure;
    private boolean startCompleted;

    public RecoveryStatus() {
    }

    RecoveryStatus(boolean hasData, long tableStamp, long tableVersionSum, int loadState, String failure,
                   boolean startCompleted) {
        this.startCompleted = startCompleted;
        this.hasData = hasData;
        this.tableStamp = tableStamp;
        this.tableVersionSum = tableVersionSum;
        this.loadState = loadState;
        this.failure = failure;
    }

    /** Whether the member found persisted metadata. */
    public boolean hasData() {
        return hasData;
    }

    public long tableStamp() {
        return tableStamp;
    }

    public long tableVersionSum() {
        return tableVersionSum;
    }

    public int loadState() {
        return loadState;
    }

    public String failure() {
        return failure;
    }

    /** Whether the member completed its start. */
    public boolean startCompleted() {
        return startCompleted;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(hasData);
        out.writeLong(tableStamp);
        out.writeLong(tableVersionSum);
        out.writeInt(loadState);
        out.writeString(failure);
        out.writeBoolean(startCompleted);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        hasData = in.readBoolean();
        tableStamp = in.readLong();
        tableVersionSum = in.readLong();
        loadState = in.readInt();
        failure = in.readString();
        startCompleted = in.readBoolean();
    }
}
