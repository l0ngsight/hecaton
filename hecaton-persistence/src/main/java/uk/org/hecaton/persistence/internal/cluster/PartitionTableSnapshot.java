/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.persistence.internal.cluster;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.PartitionReplica;
import uk.org.hecaton.internal.partition.PartitionTableView;
import uk.org.hecaton.internal.partition.ReadonlyInternalPartition;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static uk.org.hecaton.internal.partition.InternalPartition.MAX_REPLICA_COUNT;

/**
 * The partition table as persisted: per partition, its version and the UUIDs of its replicas.
 * Addresses are not kept: they may change across a restart, the UUIDs do not.
 */
public final class PartitionTableSnapshot implements DataSerializable {

    private static final int HASH_MULTIPLIER = 31;

    private int[] versions;
    private UUID[][] replicas;

    public PartitionTableSnapshot() {
    }

    private PartitionTableSnapshot(int[] versions, UUID[][] replicas) {
        this.versions = versions;
        this.replicas = replicas;
    }

    static PartitionTableSnapshot from(PartitionTableView view) {
        int count = view.length();
        int[] versions = new int[count];
        UUID[][] replicas = new UUID[count][MAX_REPLICA_COUNT];
        for (int p = 0; p < count; p++) {
            InternalPartition partition = view.getPartition(p);
            versions[p] = partition.version();
            for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
                PartitionReplica replica = partition.getReplica(i);
                replicas[p][i] = replica == null ? null : replica.uuid();
            }
        }
        return new PartitionTableSnapshot(versions, replicas);
    }

    public int partitionCount() {
        return versions.length;
    }

    /** Whether partitions were assigned. */
    public boolean hasReplicas() {
        for (UUID[] partition : replicas) {
            if (partition[0] != null) {
                return true;
            }
        }
        return false;
    }

    /** @return the replica index of the member in the partition, or -1 */
    public int replicaIndexOf(int partitionId, UUID member) {
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            if (member.equals(replicas[partitionId][i])) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The table without the replicas of members not in {@code members}; the remaining replicas of a
     * partition move up, so a backup takes the place of a missing owner.
     */
    public PartitionTableSnapshot retainOnly(Set<UUID> members) {
        UUID[][] retained = new UUID[replicas.length][MAX_REPLICA_COUNT];
        for (int p = 0; p < replicas.length; p++) {
            int next = 0;
            for (UUID replica : replicas[p]) {
                if (replica != null && members.contains(replica)) {
                    retained[p][next++] = replica;
                }
            }
        }
        return new PartitionTableSnapshot(versions.clone(), retained);
    }

    /** The members holding replicas. */
    public Set<UUID> members() {
        Set<UUID> members = new HashSet<>();
        for (UUID[] partition : replicas) {
            for (UUID replica : partition) {
                if (replica != null) {
                    members.add(replica);
                }
            }
        }
        return members;
    }

    /** A fingerprint of the table: equal tables have equal stamps. */
    public long stamp() {
        long stamp = Arrays.hashCode(versions);
        for (UUID[] partition : replicas) {
            stamp = stamp * HASH_MULTIPLIER + Arrays.hashCode(partition);
        }
        return stamp;
    }

    /** The sum of the partition versions: a table with a larger sum has seen more changes. */
    public long versionSum() {
        long sum = 0;
        for (int version : versions) {
            sum += version;
        }
        return sum;
    }

    /** The table with the members' current addresses; replicas of members not in {@code addresses} are left out. */
    public PartitionTableView toView(Map<UUID, Address> addresses) {
        InternalPartition[] partitions = new InternalPartition[versions.length];
        for (int p = 0; p < versions.length; p++) {
            PartitionReplica[] partitionReplicas = new PartitionReplica[MAX_REPLICA_COUNT];
            for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
                UUID uuid = replicas[p][i];
                Address address = uuid == null ? null : addresses.get(uuid);
                partitionReplicas[i] = address == null ? null : new PartitionReplica(address, uuid);
            }
            partitions[p] = new ReadonlyInternalPartition(partitionReplicas, p, versions[p]);
        }
        return new PartitionTableView(partitions);
    }

    void writeTo(DataOutput out) throws IOException {
        out.writeInt(versions.length);
        for (int p = 0; p < versions.length; p++) {
            out.writeInt(versions[p]);
            for (UUID replica : replicas[p]) {
                writeUuid(out, replica);
            }
        }
    }

    static PartitionTableSnapshot readFrom(DataInput in) throws IOException {
        int count = in.readInt();
        int[] versions = new int[count];
        UUID[][] replicas = new UUID[count][MAX_REPLICA_COUNT];
        for (int p = 0; p < count; p++) {
            versions[p] = in.readInt();
            for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
                replicas[p][i] = readUuid(in);
            }
        }
        return new PartitionTableSnapshot(versions, replicas);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeTo(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        PartitionTableSnapshot read = readFrom(in);
        versions = read.versions;
        replicas = read.replicas;
    }

    static void writeUuid(DataOutput out, UUID uuid) throws IOException {
        out.writeBoolean(uuid != null);
        if (uuid != null) {
            out.writeLong(uuid.getMostSignificantBits());
            out.writeLong(uuid.getLeastSignificantBits());
        }
    }

    static UUID readUuid(DataInput in) throws IOException {
        return in.readBoolean() ? new UUID(in.readLong(), in.readLong()) : null;
    }
}
