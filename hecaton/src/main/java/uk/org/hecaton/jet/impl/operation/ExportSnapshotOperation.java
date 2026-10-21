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

package uk.org.hecaton.jet.impl.operation;

import uk.org.hecaton.jet.impl.execution.init.JetInitDataSerializerHook;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Exports a snapshot of a running job under a name, optionally cancelling the job at that
 * snapshot ({@code Job#exportSnapshot}, {@code Job#cancelAndExportSnapshot}, F18).
 */
public class ExportSnapshotOperation extends AsyncMasterOnlyJobOperation {

    private String name;
    private boolean cancelJob;

    public ExportSnapshotOperation() {
    }

    public ExportSnapshotOperation(long jobId, String name, boolean cancelJob) {
        super(jobId);
        this.name = name;
        this.cancelJob = cancelJob;
    }

    @Override
    public CompletableFuture<Void> doRun() {
        return getJobCoordinationService().exportSnapshot(jobId(), name, cancelJob);
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.EXPORT_SNAPSHOT_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(name);
        out.writeBoolean(cancelJob);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readString();
        cancelJob = in.readBoolean();
    }
}
