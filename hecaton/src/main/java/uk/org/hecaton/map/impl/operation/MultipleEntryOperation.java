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

import uk.org.hecaton.core.ManagedContext;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.impl.MapDataSerializerHook;
import uk.org.hecaton.map.impl.MapEntries;
import uk.org.hecaton.map.impl.operation.steps.MultipleEntryOpSteps;
import uk.org.hecaton.map.impl.operation.steps.engine.State;
import uk.org.hecaton.map.impl.operation.steps.engine.Step;
import uk.org.hecaton.map.impl.recordstore.StaticParams;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;
import uk.org.hecaton.wan.impl.CallerProvenance;

import java.io.IOException;
import java.util.Set;

import static uk.org.hecaton.internal.util.SetUtil.createHashSet;
import static uk.org.hecaton.map.impl.operation.EntryOperator.operator;


public class MultipleEntryOperation extends MapOperation
        implements MutatingOperation, PartitionAwareOperation, BackupAwareOperation {

    protected Set<Data> keys;
    protected MapEntries responses;
    protected EntryProcessor entryProcessor;

    public MultipleEntryOperation() {
    }

    public MultipleEntryOperation(String name, Set<Data> keys, EntryProcessor entryProcessor) {
        super(name);
        this.keys = keys;
        this.entryProcessor = entryProcessor;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        final SerializationService serializationService = getNodeEngine().getSerializationService();
        final ManagedContext managedContext = serializationService.getManagedContext();
        // Namespace awareness already in place from MapOperation#beforeRun
        entryProcessor = (EntryProcessor) managedContext.initialize(entryProcessor);
    }

    @Override
    public State createState() {
        return super.createState()
                .setKeys(keys)
                .setPredicate(getPredicate())
                .setCallerProvenance(CallerProvenance.NOT_WAN)
                .setEntryProcessor(entryProcessor)
                .setStaticPutParams(StaticParams.SET_WITH_NO_ACCESS_PARAMS);
    }

    @Override
    public Step getStartingStep() {
        return MultipleEntryOpSteps.FIND_KEYS_TO_LOAD;
    }

    @Override
    public void applyState(State state) {
        super.applyState(state);
        responses = (MapEntries) state.getResult();
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    protected void runInternal() {
        responses = new MapEntries(keys.size());
        if (keys.isEmpty()) {
            return;
        }

        EntryOperator operator = operator(this, entryProcessor, getPredicate());
        for (Data key : keys) {
            Data response = operator.operateOnKey(key).doPostOperateOps().getResult();
            if (response != null) {
                responses.add(key, response);
            }
        }
    }

    protected Predicate getPredicate() {
        return null;
    }

    @Override
    public Object getResponse() {
        return responses;
    }

    @Override
    public boolean shouldBackup() {
        return mapContainer.getTotalBackupCount() > 0 && entryProcessor.getBackupProcessor() != null;
    }

    @Override
    public int getSyncBackupCount() {
        return 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getTotalBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        EntryProcessor backupProcessor = entryProcessor.getBackupProcessor();
        MultipleEntryBackupOperation backupOperation = null;
        if (backupProcessor != null) {
            backupOperation = new MultipleEntryBackupOperation(name, keys, backupProcessor);
        }
        return backupOperation;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = callWithNamespaceAwareness(in::readObject);
        int size = in.readInt();
        keys = createHashSet(size);
        for (int i = 0; i < size; i++) {
            Data key = IOUtil.readData(in);
            keys.add(key);
        }

    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
        out.writeInt(keys.size());
        for (Data key : keys) {
            IOUtil.writeData(out, key);
        }
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MULTIPLE_ENTRY;
    }
}
