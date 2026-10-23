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

import uk.org.hecaton.core.EntryEventType;
import uk.org.hecaton.map.impl.MapDataSerializerHook;
import uk.org.hecaton.map.impl.operation.steps.ClearOpSteps;
import uk.org.hecaton.map.impl.operation.steps.engine.State;
import uk.org.hecaton.map.impl.operation.steps.engine.Step;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;

import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;

public class ClearOperation extends MapOperation
        implements BackupAwareOperation, PartitionAwareOperation, MutatingOperation {

    private boolean shouldBackup;
    private int numberOfClearedEntries;

    public ClearOperation() {
        this(null);
    }

    public ClearOperation(String name) {
        super(name);
        createRecordStoreOnDemand = false;
    }

    @Override
    protected void runInternal() {
        if (recordStore == null) {
            return;
        }

        numberOfClearedEntries = recordStore.clear(false);
        shouldBackup = true;
    }

    @Override
    public Step getStartingStep() {
        return ClearOpSteps.CLEAR_MEMORY;
    }

    @Override
    public State createState() {
        return super.createState()
                .setResult(0);
    }

    @Override
    public void applyState(State state) {
        if (recordStore == null) {
            return;
        }
        super.applyState(state);
        numberOfClearedEntries = (int) state.getResult();
        shouldBackup = true;
    }

    @Override
    public void afterRunInternal() {
        if (mapContainer != null) {
            invalidateAllKeysInNearCaches();
        }
        hintMapEvent();
    }

    private void hintMapEvent() {
        mapEventPublisher.hintMapEvent(getCallerAddress(), name, EntryEventType.CLEAR_ALL,
                numberOfClearedEntries, getPartitionId());
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public int getSyncBackupCount() {
        return mapServiceContext.getMapContainer(name).getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return mapServiceContext.getMapContainer(name).getAsyncBackupCount();
    }

    @Override
    public Object getResponse() {
        return numberOfClearedEntries;
    }

    @Override
    public Operation getBackupOperation() {
        return new ClearBackupOperation(name)
                .setServiceName(SERVICE_NAME);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.CLEAR;
    }

    @Override
    protected ImmutableMapAccess immutableMapAccess() {
        // removes all entries
        return ImmutableMapAccess.CHANGE;
    }
}
