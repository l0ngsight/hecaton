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

import uk.org.hecaton.internal.partition.IPartition;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.PartitionReplica;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.spi.impl.NodeEngine;

import java.util.Collection;
import java.util.function.BiConsumer;

import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

// runs on the partition owner
final class CheckPartitionReplicaVersionTask extends AbstractPartitionPrimaryReplicaAntiEntropyTask {

    private final int replicaIndex;

    private final BiConsumer<Object, Throwable> callback;

    CheckPartitionReplicaVersionTask(NodeEngine nodeEngine, int partitionId, int replicaIndex,
                                     BiConsumer<Object, Throwable> callback) {
        super(nodeEngine, partitionId);
        if (replicaIndex < 1 || replicaIndex > IPartition.MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Replica index must be in range [1-"
                    + IPartition.MAX_BACKUP_COUNT + "]");
        }
        this.replicaIndex = replicaIndex;
        checkNotNull(callback);
        this.callback = callback;
    }

    @Override
    public void run() {
        InternalPartition partition = partitionService.getPartition(partitionId);
        if (!partition.isLocal() || partition.isMigrating()) {
            callback.accept(false, null);
            return;
        }

        Collection<ServiceNamespace> namespaces = retainAndGetNamespaces();
        PartitionReplica target = partition.getReplica(replicaIndex);
        if (target == null) {
            callback.accept(false, null);
            return;
        }

        invokePartitionBackupReplicaAntiEntropyOp(replicaIndex, target, namespaces, callback);
    }

}
