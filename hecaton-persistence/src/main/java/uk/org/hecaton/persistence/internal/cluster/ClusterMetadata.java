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

import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.internal.partition.PartitionTableView;
import uk.org.hecaton.persistence.internal.store.AtomicFiles;
import uk.org.hecaton.persistence.internal.store.PersistenceStoreException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * The cluster metadata a member keeps on disk: the member list, the partition table and the
 * cluster state, each in its own file written atomically.
 */
public final class ClusterMetadata {

    private static final String MEMBERS = "members";
    private static final String PARTITIONS = "partitions";
    private static final String STATE = "cluster-state";

    private final Path dir;

    public ClusterMetadata(Path dir) {
        this.dir = dir;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot create " + dir, e);
        }
    }

    public void writeMembers(int version, Collection<? extends Member> members) {
        write(MEMBERS, out -> {
            out.writeInt(version);
            out.writeInt(members.size());
            for (Member member : members) {
                PartitionTableSnapshot.writeUuid(out, member.getUuid());
                out.writeUTF(member.getAddress().toString());
                out.writeBoolean(member.isLiteMember());
            }
        });
    }

    /** @return the persisted member list, or null */
    public PersistedMembers readMembers() {
        return read(MEMBERS, in -> {
            int version = in.readInt();
            int count = in.readInt();
            List<PersistedMember> members = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                members.add(new PersistedMember(PartitionTableSnapshot.readUuid(in), in.readUTF(), in.readBoolean()));
            }
            return new PersistedMembers(version, members);
        });
    }

    public void writePartitions(PartitionTableView view) {
        PartitionTableSnapshot snapshot = PartitionTableSnapshot.from(view);
        write(PARTITIONS, snapshot::writeTo);
    }

    /** @return the persisted partition table, or null */
    public PartitionTableSnapshot readPartitions() {
        return read(PARTITIONS, PartitionTableSnapshot::readFrom);
    }

    public void writeState(ClusterState state) {
        AtomicFiles.writeString(dir.resolve(STATE), state.name());
    }

    /** @return the persisted cluster state, or null */
    public ClusterState readState() {
        String state = AtomicFiles.readString(dir.resolve(STATE));
        return state == null ? null : ClusterState.valueOf(state.trim());
    }

    /** Deletes the metadata. */
    public void delete() {
        for (String file : List.of(MEMBERS, PARTITIONS, STATE)) {
            try {
                Files.deleteIfExists(dir.resolve(file));
            } catch (IOException e) {
                throw new PersistenceStoreException("Cannot delete " + dir.resolve(file), e);
            }
        }
    }

    private void write(String file, Writer writer) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writer.write(out);
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot write " + file, e);
        }
        AtomicFiles.write(dir.resolve(file), bytes.toByteArray());
    }

    private <T> T read(String file, Reader<T> reader) {
        byte[] bytes = AtomicFiles.read(dir.resolve(file));
        if (bytes == null) {
            return null;
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            return reader.read(in);
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot read " + dir.resolve(file), e);
        }
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream out) throws IOException;
    }

    @FunctionalInterface
    private interface Reader<T> {
        T read(DataInputStream in) throws IOException;
    }

    /**
     * A member of the persisted member list.
     *
     * @param uuid       its UUID
     * @param address    its address then
     * @param liteMember whether it was a lite member
     */
    public record PersistedMember(UUID uuid, String address, boolean liteMember) {
    }

    /**
     * The persisted member list.
     *
     * @param version its version
     * @param members the members
     */
    public record PersistedMembers(int version, List<PersistedMember> members) {
    }
}
