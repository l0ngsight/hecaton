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

package uk.org.hecaton.internal.partition.operation;

import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.partition.MigrationCycleOperation;
import uk.org.hecaton.internal.partition.ReplicaErrorLogger;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.internal.partition.impl.PartitionDataSerializerHook;
import uk.org.hecaton.internal.partition.impl.PartitionReplicaManager;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.operationservice.BackupOperation;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;
import uk.org.hecaton.internal.services.ServiceNamespace;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static uk.org.hecaton.internal.serialization.impl.SerializationUtil.readCollection;
import static uk.org.hecaton.internal.serialization.impl.SerializationUtil.writeCollection;

/**
 * The response to a {@link PartitionReplicaSyncRequest} that the replica should retry. This will reset the current ongoing
 * synchronization request state and retry the request if this node is still a replica of this partition.
 */
public class PartitionReplicaSyncRetryResponse
        extends AbstractPartitionOperation
        implements PartitionAwareOperation, BackupOperation, MigrationCycleOperation {

    private Collection<ServiceNamespace> namespaces;

    public PartitionReplicaSyncRetryResponse() {
        namespaces = Collections.emptySet();
    }

    public PartitionReplicaSyncRetryResponse(Collection<ServiceNamespace> namespaces) {
        this.namespaces = namespaces;
    }

    @Override
    public void run() throws Exception {
        final InternalPartitionServiceImpl partitionService = getService();
        final int partitionId = getPartitionId();
        final int replicaIndex = getReplicaIndex();

        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        for (ServiceNamespace namespace : namespaces) {
            replicaManager.clearReplicaSyncRequest(partitionId, namespace, replicaIndex);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public String getServiceName() {
        return IPartitionService.SERVICE_NAME;
    }

    @Override
    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        writeCollection(namespaces, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        namespaces = readCollection(in);
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.REPLICA_SYNC_RETRY_RESPONSE;
    }
}
