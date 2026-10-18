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

package uk.org.hecaton.durableexecutor.impl.operations;

import uk.org.hecaton.durableexecutor.impl.DistributedDurableExecutorService;
import uk.org.hecaton.durableexecutor.impl.DurableExecutorContainer;
import uk.org.hecaton.durableexecutor.impl.DurableExecutorDataSerializerHook;
import uk.org.hecaton.durableexecutor.impl.DurableExecutorPartitionContainer;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.AbstractNamedOperation;

abstract class AbstractDurableExecutorOperation extends AbstractNamedOperation implements IdentifiedDataSerializable {

    private transient DurableExecutorContainer executorContainer;

    AbstractDurableExecutorOperation() {
    }

    AbstractDurableExecutorOperation(String name) {
        super(name);
    }

    @Override
    public String getServiceName() {
        return DistributedDurableExecutorService.SERVICE_NAME;
    }

    public DurableExecutorContainer getExecutorContainer() {
        if (executorContainer == null) {
            DistributedDurableExecutorService service = getService();
            DurableExecutorPartitionContainer partitionContainer = service.getPartitionContainer(getPartitionId());
            executorContainer = partitionContainer.getOrCreateContainer(name);
        }
        return executorContainer;
    }

    public int getSyncBackupCount() {
        return executorContainer.getDurability();
    }

    public int getAsyncBackupCount() {
        return 0;
    }

    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getFactoryId() {
        return DurableExecutorDataSerializerHook.F_ID;
    }
}
