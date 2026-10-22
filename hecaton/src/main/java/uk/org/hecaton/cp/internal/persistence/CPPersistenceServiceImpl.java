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

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.cp.CPSubsystemConfig;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.cp.CPMember;
import uk.org.hecaton.cp.internal.CPMemberInfo;
import uk.org.hecaton.cp.internal.MetadataRaftGroupManager;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.RaftService;
import uk.org.hecaton.cp.internal.raft.impl.persistence.LogFileStructure;
import uk.org.hecaton.cp.internal.raft.impl.persistence.RaftStateStore;
import uk.org.hecaton.cp.internal.raft.impl.persistence.RestoredRaftState;
import uk.org.hecaton.cp.internal.raftop.metadata.RestartCPMemberOp;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.cp.internal.RaftServiceUtil.CP_MEMBER_UUID_ATTRIBUTE;
import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;

/**
 * CP Subsystem persistence (F9): the member's CP identity and the state of its Raft nodes are kept
 * in a directory of its own under the CP {@code base-dir}, locked while the member runs. When the
 * member restarts, it takes the directory of an earlier member and restores:
 * <ol>
 *     <li>before it joins the cluster: its CP member (with its current address) and the METADATA
 *     group ID, so that it joins as that CP member;</li>
 *     <li>once it has joined: the active CP members and the Raft nodes of its CP groups, which
 *     then catch up with their groups. The member's start completes after that.</li>
 * </ol>
 * A member that restarts with another address has the active CP members list updated through the
 * METADATA group.
 */
public final class CPPersistenceServiceImpl implements CPPersistenceService {

    private static final String LOCK_FILE = "lock";
    private static final String GROUPS_DIR = "groups";
    private static final String GROUP_ID_FILE = "group-id";
    private static final long RESTART_OP_RETRY_SECONDS = 1;

    private final Node node;
    private final ILogger logger;
    private final InternalSerializationService serializationService;
    private final Path dir;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final FileCPMetadataStore metadataStore;
    /** The logs read at the start, for the Raft nodes being restored. */
    private final Map<RaftGroupId, RaftLogFile> restoredLogs = new ConcurrentHashMap<>();
    private volatile boolean restoreCompleted;
    private CPMemberInfo restartedMember;

    public CPPersistenceServiceImpl(Node node) {
        this.node = node;
        this.logger = node.getLogger(CPPersistenceServiceImpl.class);
        this.serializationService = node.getSerializationService();
        CPSubsystemConfig config = node.getConfig().getCPSubsystemConfig();
        Path baseDir = config.getBaseDir().toPath().toAbsolutePath();
        try {
            Files.createDirectories(baseDir);
            Claimed claimed = claim(baseDir);
            this.dir = claimed.dir;
            this.lockChannel = claimed.channel;
            this.lock = claimed.lock;
        } catch (IOException e) {
            throw new HecatonException("Cannot claim a CP persistence directory in " + baseDir, e);
        }
        this.metadataStore = new FileCPMetadataStore(dir, serializationService);
        logger.info("CP Subsystem persistence uses " + dir);
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public CPMetadataStore getCPMetadataStore() {
        return metadataStore;
    }

    @Override
    public RaftStateStore createRaftStateStore(@Nonnull RaftGroupId groupId, @Nullable LogFileStructure logFileStructure) {
        Path groupDir = groupDir(groupId);
        try {
            Files.createDirectories(groupDir);
            if (!Files.exists(groupDir.resolve(GROUP_ID_FILE))) {
                CPFiles.write(groupDir.resolve(GROUP_ID_FILE), CPFiles.serialize(serializationService, groupId));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new FileRaftStateStore(groupDir, serializationService, restoredLogs.remove(groupId));
    }

    @Override
    public void removeRaftStateStore(@Nonnull RaftGroupId groupId) {
        try {
            CPFiles.deleteRecursively(groupDir(groupId));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void reset() {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir)) {
            for (Path file : files) {
                if (!file.getFileName().toString().equals(LOCK_FILE)) {
                    CPFiles.deleteRecursively(file);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Whether the CP state has been restored (or there was none): the member's start can complete. */
    public boolean isRestoreCompleted() {
        return restoreCompleted;
    }

    /** Restores the member's CP identity, before it joins the cluster. */
    public void beforeJoin() {
        try {
            CPMemberInfo member = (CPMemberInfo) metadataStore.readLocalCPMember();
            if (member == null) {
                return;
            }
            MetadataRaftGroupManager metadataGroupManager = raftService().getMetadataGroupManager();
            RaftGroupId metadataGroupId = metadataStore.readMetadataGroupId();
            if (metadataGroupId != null) {
                metadataGroupManager.restoreMetadataGroupId(metadataGroupId);
            }
            Address address = node.getThisAddress();
            if (!address.equals(member.getAddress())) {
                logger.info("The CP member " + member + " restarts with the address " + address);
                member = new CPMemberInfo(member.getUuid(), address, member.isAutoStepDownWhenLeader());
                metadataStore.persistLocalCPMember(member);
                restartedMember = member;
            }
            metadataGroupManager.restoreLocalCPMember(member);
            // so that the other members find the CP member at its current address
            node.getLocalMember().setAttribute(CP_MEMBER_UUID_ATTRIBUTE, member.getUuid().toString());
            logger.info("Restored the local CP member " + member);
        } catch (IOException e) {
            throw new HecatonException("Cannot restore the CP member from " + dir, e);
        }
    }

    /** Restores the Raft nodes of the member's CP groups, once it has joined the cluster. */
    public void afterJoin() {
        try {
            CPMemberInfo member = (CPMemberInfo) metadataStore.readLocalCPMember();
            if (member != null) {
                restoreRaftNodes(member);
            }
        } catch (IOException e) {
            throw new HecatonException("Cannot restore the CP state from " + dir, e);
        } finally {
            restoreCompleted = true;
        }
        if (restartedMember != null) {
            updateAddress(restartedMember);
        }
    }

    private void restoreRaftNodes(CPMemberInfo localMember) throws IOException {
        RaftService raftService = raftService();
        RaftGroupId metadataGroupId = raftService.getMetadataGroupId();
        List<CPMember> members = new ArrayList<>();
        long commitIndex = metadataStore.readActiveCPMembers(members);
        if (!members.isEmpty()) {
            members.replaceAll(m -> m.getUuid().equals(localMember.getUuid()) ? localMember : m);
            raftService.updateInvocationManagerMembers(metadataGroupId.getSeed(), commitIndex, members);
            for (Member member : node.getClusterService().getMembers()) {
                raftService.updateRestartedCPMemberAddress(member);
            }
        }
        Path groups = dir.resolve(GROUPS_DIR);
        if (!Files.isDirectory(groups)) {
            return;
        }
        List<Path> groupDirs = new ArrayList<>();
        try (DirectoryStream<Path> dirs = Files.newDirectoryStream(groups, Files::isDirectory)) {
            dirs.forEach(groupDirs::add);
        }
        for (Path groupDir : groupDirs) {
            byte[] groupIdBytes = CPFiles.read(groupDir.resolve(GROUP_ID_FILE));
            RaftGroupId groupId = groupIdBytes == null ? null
                    : CPFiles.input(serializationService, groupIdBytes).readObject();
            if (groupId == null || groupId.getSeed() != metadataGroupId.getSeed()
                    || !FileRaftStateLoader.hasState(groupDir)) {
                // a group of an earlier CP Subsystem (reset since), or one whose node never started
                CPFiles.deleteRecursively(groupDir);
                continue;
            }
            FileRaftStateLoader loader = new FileRaftStateLoader(groupDir, serializationService);
            RestoredRaftState state = loader.load();
            restoredLogs.put(groupId, loader.logFile());
            raftService.restoreRaftNode(groupId, state, null);
        }
    }

    /** Has the METADATA group replace the member's address in the active CP members. */
    private void updateAddress(CPMemberInfo member) {
        RaftService raftService = raftService();
        raftService.getInvocationManager()
                .invoke(raftService.getMetadataGroupId(), new RestartCPMemberOp(member))
                .whenCompleteAsync((result, failure) -> {
                    if (failure == null) {
                        logger.info("The active CP members have the new address of " + member);
                    } else if (failure.getCause() instanceof IllegalStateException
                            || failure instanceof IllegalStateException) {
                        logger.severe("Cannot update the address of " + member + " in the active CP members", failure);
                    } else if (node.isRunning()) {
                        logger.fine("Retrying the address update of " + member + ": " + failure);
                        node.getNodeEngine().getExecutionService().schedule(() -> updateAddress(member),
                                RESTART_OP_RETRY_SECONDS, TimeUnit.SECONDS);
                    }
                }, CALLER_RUNS);
    }

    /** Releases the member's directory (at shutdown). */
    public void close() {
        try {
            lock.release();
            lockChannel.close();
        } catch (IOException e) {
            logger.warning("Cannot release " + dir, e);
        }
    }

    private RaftService raftService() {
        return node.getNodeEngine().getService(RaftService.SERVICE_NAME);
    }

    private Path groupDir(RaftGroupId groupId) {
        return dir.resolve(GROUPS_DIR).resolve(Long.toHexString(groupId.getSeed()) + "-" + Long.toHexString(groupId.getId()));
    }

    /**
     * Claims the directory of an earlier member under {@code baseDir} that no running member holds
     * (one that holds a CP identity first), or creates a new one.
     */
    private static Claimed claim(Path baseDir) throws IOException {
        List<Path> withIdentity = new ArrayList<>();
        List<Path> others = new ArrayList<>();
        try (DirectoryStream<Path> dirs = Files.newDirectoryStream(baseDir, Files::isDirectory)) {
            for (Path candidate : dirs) {
                boolean identity = Files.exists(candidate.resolve(FileCPMetadataStore.CP_MEMBER_FILE))
                        || Files.exists(candidate.resolve(FileCPMetadataStore.AP_MEMBER_FILE));
                (identity ? withIdentity : others).add(candidate);
            }
        }
        withIdentity.sort(null);
        others.sort(null);
        withIdentity.addAll(others);
        for (Path candidate : withIdentity) {
            Claimed claimed = tryLock(candidate);
            if (claimed != null) {
                return claimed;
            }
        }
        Path dir = baseDir.resolve(UuidUtil.newUnsecureUUID().toString());
        Files.createDirectories(dir);
        Claimed claimed = tryLock(dir);
        if (claimed == null) {
            throw new IOException("Cannot lock the new directory " + dir);
        }
        return claimed;
    }

    private static Claimed tryLock(Path dir) throws IOException {
        FileChannel channel = FileChannel.open(dir.resolve(LOCK_FILE), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock lock;
        try {
            lock = channel.tryLock();
        } catch (OverlappingFileLockException e) {
            // held by another member in this JVM
            lock = null;
        }
        if (lock == null) {
            channel.close();
            return null;
        }
        return new Claimed(dir, channel, lock);
    }

    private record Claimed(Path dir, FileChannel channel, FileLock lock) {
    }
}
