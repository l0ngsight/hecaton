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

import uk.org.hecaton.internal.hotrestart.InternalHotRestartService;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.AllowedDuringPassiveState;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.UrgentSystemOperation;

import java.io.IOException;

/**
 * An operation of the recovery protocol, sent by the master to the members (see
 * {@link RecoveryService}). {@link #STATUS} returns the member's {@link RecoveryStatus};
 * {@link #LOAD} asks it to load its data for the given partition table; {@link #COMPLETE} installs
 * the partition table and ends recovery.
 */
public final class RecoveryOperation extends Operation implements AllowedDuringPassiveState, UrgentSystemOperation {

    static final int STATUS = 1;
    static final int LOAD = 2;
    static final int COMPLETE = 3;

    private int step;
    private PartitionTableSnapshot table;
    private Object response;

    public RecoveryOperation() {
    }

    RecoveryOperation(int step, PartitionTableSnapshot table) {
        this.step = step;
        this.table = table;
    }

    @Override
    public String getServiceName() {
        return InternalHotRestartService.SERVICE_NAME;
    }

    @Override
    public void run() {
        RecoveryService service = getService();
        switch (step) {
            case STATUS -> response = service.status();
            case LOAD -> service.startLoading(table);
            case COMPLETE -> service.complete(table);
            default -> throw new IllegalStateException("Unknown recovery step " + step);
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(step);
        out.writeBoolean(table != null);
        if (table != null) {
            table.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        step = in.readInt();
        if (in.readBoolean()) {
            table = new PartitionTableSnapshot();
            table.readData(in);
        }
    }
}
