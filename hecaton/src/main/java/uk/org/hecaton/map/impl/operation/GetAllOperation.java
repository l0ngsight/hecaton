/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.map.impl.operation;

import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.impl.MapDataSerializerHook;
import uk.org.hecaton.map.impl.MapEntries;
import uk.org.hecaton.map.impl.operation.steps.GetAllOpSteps;
import uk.org.hecaton.map.impl.operation.steps.engine.Step;
import uk.org.hecaton.map.impl.operation.steps.engine.State;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static uk.org.hecaton.internal.util.SetUtil.createHashSet;

public class GetAllOperation extends MapOperation
        implements ReadonlyOperation, PartitionAwareOperation {

    /**
     * Speculative factor to be used when initialising collections
     * of an approximate final size.
     */
    private static final double SIZING_FUDGE_FACTOR = 1.3;

    private List<Data> keys = new ArrayList<>();
    private MapEntries entries;

    public GetAllOperation() {
    }

    public GetAllOperation(String name, List<Data> keys) {
        super(name);
        this.keys = keys;
    }

    @Override
    protected void innerBeforeRun() throws Exception {
        super.innerBeforeRun();
        if (recordStore != null) {
            recordStore.checkIfLoaded();
        }
    }

    @Override
    protected void runInternal() {
        Set<Data> partitionKeySet = getPartitionKeySet(keys);
        entries = recordStore.getAll(partitionKeySet, getCallerAddress());
    }

    public Set<Data> getPartitionKeySet(List<Data> keys) {
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        int partitionId = getPartitionId();
        final int roughSize = (int) (keys.size() * SIZING_FUDGE_FACTOR / partitionService.getPartitionCount());
        Set<Data> partitionKeySet = createHashSet(roughSize);
        for (Data key : keys) {
            if (partitionId == partitionService.getPartitionId(key)) {
                partitionKeySet.add(key);
            }
        }
        return partitionKeySet;
    }

    @Override
    public State createState() {
        return super.createState()
                .setKeys(keys);
    }

    @Override
    public Step getStartingStep() {
        return GetAllOpSteps.READ;
    }

    @Override
    public void applyState(State state) {
        super.applyState(state);
        entries = state.getMapEntries();
    }

    @Override
    public Object getResponse() {
        return entries;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (keys == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(keys.size());
            for (Data key : keys) {
                IOUtil.writeData(out, key);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        if (size > -1) {
            for (int i = 0; i < size; i++) {
                Data data = IOUtil.readData(in);
                keys.add(data);
            }
        }
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.GET_ALL;
    }
}
