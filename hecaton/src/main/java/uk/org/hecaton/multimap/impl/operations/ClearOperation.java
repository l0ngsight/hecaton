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

package uk.org.hecaton.multimap.impl.operations;

import uk.org.hecaton.multimap.impl.MultiMapContainer;
import uk.org.hecaton.multimap.impl.MultiMapDataSerializerHook;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;

public class ClearOperation extends AbstractMultiMapOperation implements BackupAwareOperation, PartitionAwareOperation,
        MutatingOperation {

    private transient MultiMapContainer container;
    private transient boolean shouldBackup;

    public ClearOperation() {
    }

    public ClearOperation(String name) {
        super(name);
    }

    @Override
    public void beforeRun() throws Exception {
        container = getOrCreateContainer();
        shouldBackup = container.size() > 0;
    }

    @Override
    public void run() throws Exception {
        container = getOrCreateContainer();
        response = container.clear();
    }

    @Override
    public void afterRun() throws Exception {
        if (shouldBackup) {
            container.update();
        }
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public Operation getBackupOperation() {
        return new ClearBackupOperation(name);
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.CLEAR;
    }
}
