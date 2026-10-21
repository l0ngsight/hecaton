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
package uk.org.hecaton.persistence.internal;

import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.hotrestart.InternalHotRestartService;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.AllowedDuringPassiveState;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;

/** Starts (or, with {@link #INTERRUPT}, interrupts) a member's part of a backup. */
public final class BackupOperation extends Operation implements AllowedDuringPassiveState {

    /** The sequence that interrupts the running backup. */
    static final long INTERRUPT = -1;

    private long backupSeq;

    public BackupOperation() {
    }

    BackupOperation(long backupSeq) {
        this.backupSeq = backupSeq;
    }

    BackupOperation copy() {
        return new BackupOperation(backupSeq);
    }

    @Override
    public String getServiceName() {
        return InternalHotRestartService.SERVICE_NAME;
    }

    @Override
    public void run() {
        Node node = ((NodeEngineImpl) getNodeEngine()).getNode();
        BackupService service = (BackupService) node.getNodeExtension().getHotRestartService();
        if (backupSeq == INTERRUPT) {
            service.interruptLocalBackupTask();
        } else {
            service.startLocalBackup(backupSeq);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(backupSeq);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        backupSeq = in.readLong();
    }
}
