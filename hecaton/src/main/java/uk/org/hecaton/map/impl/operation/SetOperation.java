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

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.impl.MapDataSerializerHook;
import uk.org.hecaton.map.impl.operation.steps.engine.State;
import uk.org.hecaton.map.impl.recordstore.StaticParams;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;

import static uk.org.hecaton.core.EntryEventType.ADDED;
import static uk.org.hecaton.core.EntryEventType.UPDATED;

public class SetOperation extends BasePutOperation implements MutatingOperation {

    private transient boolean newRecord;

    public SetOperation() {
    }

    public SetOperation(String name, Data dataKey, Data value) {
        super(name, dataKey, value);
    }

    @Override
    protected void runInternal() {
        oldValue = recordStore.set(dataKey, dataValue, getTtl(), getMaxIdle());
        newRecord = oldValue == null;
    }

    @Override
    public void applyState(State state) {
        super.applyState(state);
        newRecord = state.getOldValue() == null;
    }

    @Override
    protected StaticParams getStaticParams() {
        return StaticParams.SET_PARAMS;
    }

    @Override
    public void afterRunInternal() {
        eventType = newRecord ? ADDED : UPDATED;

        super.afterRunInternal();
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.SET;
    }
}
