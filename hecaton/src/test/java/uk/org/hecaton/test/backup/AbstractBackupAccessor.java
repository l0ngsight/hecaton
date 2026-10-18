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

package uk.org.hecaton.test.backup;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.IPartition;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.InternalPartitionService;

import java.util.Arrays;

import static uk.org.hecaton.test.Accessors.getNode;

/**
 * Base class for {@link BackupAccessor} implementations.
 *
 * @param <K> type of keys
 * @param <V> type of values
 */
public abstract class AbstractBackupAccessor<K, V> implements BackupAccessor<K, V> {

    protected final HecatonInstance[] cluster;
    protected final int replicaIndex;

    protected AbstractBackupAccessor(HecatonInstance[] cluster, int replicaIndex) {
        if (cluster == null || cluster.length == 0) {
            throw new IllegalArgumentException("Cluster has to have at least 1 member.");
        }
        if (replicaIndex > IPartition.MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Cannot access replica index " + replicaIndex);
        }
        this.cluster = cluster;
        this.replicaIndex = replicaIndex;
    }

    protected HecatonInstance getHecatonInstance(IPartition partition) {
        Address replicaAddress = partition.getReplicaAddress(replicaIndex);
        if (replicaAddress == null) {
            // there is no owner of this replica (yet?)
            return null;
        }

        HecatonInstance hz = getInstanceWithAddress(replicaAddress);
        if (hz == null) {
            throw new IllegalStateException("Partition " + partition + " with replica index " + replicaIndex
                    + " is mapped to " + replicaAddress + " but there is no member with this address in the cluster."
                    + " List of known members: " + Arrays.toString(cluster));
        }
        return hz;
    }

    protected InternalPartition getPartitionForKey(K key) {
        // to determine partition we can use any instance (let's pick the first one)
        HecatonInstance instance = cluster[0];
        InternalPartitionService partitionService = getNode(instance).getPartitionService();
        int partitionId = partitionService.getPartitionId(key);
        return partitionService.getPartition(partitionId);
    }

    protected HecatonInstance getInstanceWithAddress(Address address) {
        for (HecatonInstance hz : cluster) {
            if (hz.getCluster().getLocalMember().getAddress().equals(address)) {
                return hz;
            }
        }
        throw new IllegalStateException("Address " + address + " not found in the cluster");
    }

    @Override
    public String toString() {
        return "AbstractBackupAccessor{"
                + "replicaIndex=" + replicaIndex + '}';
    }
}
