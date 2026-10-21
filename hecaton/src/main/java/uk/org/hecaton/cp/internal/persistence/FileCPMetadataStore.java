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

package uk.org.hecaton.cp.internal.persistence;

import uk.org.hecaton.cp.CPMember;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.internal.nio.BufferObjectDataInput;
import uk.org.hecaton.internal.serialization.InternalSerializationService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The CP identity of the member, in its CP directory: {@code cp-member} (the local CP member),
 * {@code ap-member} (a marker: the member is not a CP member), {@code active-members} (the active
 * CP members and the commit index of that list) and {@code metadata-group-id}.
 */
final class FileCPMetadataStore implements CPMetadataStore {

    static final String CP_MEMBER_FILE = "cp-member";
    static final String AP_MEMBER_FILE = "ap-member";
    static final String ACTIVE_MEMBERS_FILE = "active-members";
    static final String METADATA_GROUP_ID_FILE = "metadata-group-id";

    private final Path dir;
    private final InternalSerializationService serializationService;

    FileCPMetadataStore(Path dir, InternalSerializationService serializationService) {
        this.dir = dir;
        this.serializationService = serializationService;
    }

    @Override
    public boolean isMarkedAPMember() {
        return Files.exists(dir.resolve(AP_MEMBER_FILE));
    }

    @Override
    public synchronized boolean tryMarkAPMember() throws IOException {
        if (containsLocalMemberFile()) {
            return false;
        }
        if (!isMarkedAPMember()) {
            CPFiles.write(dir.resolve(AP_MEMBER_FILE), new byte[0]);
        }
        return true;
    }

    @Override
    public boolean containsLocalMemberFile() {
        return Files.exists(dir.resolve(CP_MEMBER_FILE));
    }

    @Override
    public synchronized void persistLocalCPMember(CPMember member) throws IOException {
        CPFiles.write(dir.resolve(CP_MEMBER_FILE), CPFiles.serialize(serializationService, member));
        // a member promoted to CP member is no longer an AP member
        Files.deleteIfExists(dir.resolve(AP_MEMBER_FILE));
    }

    @Override
    public CPMember readLocalCPMember() throws IOException {
        byte[] bytes = CPFiles.read(dir.resolve(CP_MEMBER_FILE));
        return bytes == null ? null : CPFiles.input(serializationService, bytes).readObject();
    }

    @Override
    public synchronized void persistActiveCPMembers(Collection<? extends CPMember> members, long commitIndex)
            throws IOException {
        List<Object> objects = new ArrayList<>(members.size() + 2);
        objects.add(commitIndex);
        objects.add(members.size());
        objects.addAll(members);
        CPFiles.write(dir.resolve(ACTIVE_MEMBERS_FILE), CPFiles.serialize(serializationService, objects.toArray()));
    }

    @Override
    public long readActiveCPMembers(Collection<CPMember> members) throws IOException {
        byte[] bytes = CPFiles.read(dir.resolve(ACTIVE_MEMBERS_FILE));
        if (bytes == null) {
            return 0;
        }
        BufferObjectDataInput in = CPFiles.input(serializationService, bytes);
        long commitIndex = in.readObject();
        int count = in.readObject();
        for (int i = 0; i < count; i++) {
            members.add(in.readObject());
        }
        return commitIndex;
    }

    @Override
    public synchronized void persistMetadataGroupId(RaftGroupId groupId) throws IOException {
        CPFiles.write(dir.resolve(METADATA_GROUP_ID_FILE), CPFiles.serialize(serializationService, groupId));
    }

    @Override
    public RaftGroupId readMetadataGroupId() throws IOException {
        byte[] bytes = CPFiles.read(dir.resolve(METADATA_GROUP_ID_FILE));
        return bytes == null ? null : CPFiles.input(serializationService, bytes).readObject();
    }
}
