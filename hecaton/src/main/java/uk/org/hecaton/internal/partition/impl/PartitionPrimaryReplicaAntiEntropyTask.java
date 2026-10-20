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

package uk.org.hecaton.internal.partition.impl;

import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.PartitionReplica;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.spi.impl.NodeEngineImpl;

import java.util.Collection;

import static uk.org.hecaton.internal.partition.InternalPartition.MAX_REPLICA_COUNT;

final class PartitionPrimaryReplicaAntiEntropyTask extends AbstractPartitionPrimaryReplicaAntiEntropyTask {

    private final Runnable afterRun;

    PartitionPrimaryReplicaAntiEntropyTask(NodeEngineImpl nodeEngine, int partitionId, Runnable afterRun) {
        super(nodeEngine, partitionId);
        this.afterRun = afterRun;
    }

    @Override
    public void run() {
        try {
            InternalPartition partition = partitionService.getPartition(partitionId, false);
            if (!partition.isLocal() || partition.isMigrating()) {
                return;
            }

            Collection<ServiceNamespace> namespaces = retainAndGetNamespaces();

            for (int index = 1; index < MAX_REPLICA_COUNT; index++) {
                PartitionReplica replica = partition.getReplica(index);
                if (replica != null) {
                    invokePartitionBackupReplicaAntiEntropyOp(index, replica, namespaces, null);
                }
            }
        } finally {
            if (afterRun != null) {
                afterRun.run();
            }
        }
    }
}
