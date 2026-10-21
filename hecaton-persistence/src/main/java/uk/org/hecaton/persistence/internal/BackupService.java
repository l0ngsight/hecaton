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
package uk.org.hecaton.persistence.internal;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.PersistenceConfig;
import uk.org.hecaton.hotrestart.HotRestartService;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.hotrestart.InternalHotRestartService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.persistence.BackupTaskState;
import uk.org.hecaton.persistence.BackupTaskStatus;
import uk.org.hecaton.persistence.internal.encryption.DataKey;
import uk.org.hecaton.persistence.internal.store.PersistenceStoreException;
import uk.org.hecaton.spi.impl.PartitionSpecificRunnable;
import uk.org.hecaton.spi.impl.operationservice.OperationService;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hot backups (F10.5): every member copies its persisted data, as it is at the moment each
 * partition is copied, to {@code <backup-dir>/backup-<seq>/<member directory>}. A backup is
 * restored by starting the members with it as their base directory.
 */
public final class BackupService implements HotRestartService {

    private static final BackupTaskStatus NO_TASK = new BackupTaskStatus(BackupTaskState.NO_TASK, 0, 0);

    private final Node node;
    private final ILogger logger;
    private final PersistenceConfig config;
    private final DataStore store;
    private final Path memberDir;
    private final int partitionCount;
    private final AtomicInteger completed = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();
    private volatile BackupTaskState state = BackupTaskState.NO_TASK;
    private volatile boolean interrupted;

    BackupService(Node node, DataStore store, Path memberDir, int partitionCount) {
        this.node = node;
        this.logger = node.getLogger(BackupService.class);
        this.config = node.getConfig().getPersistenceConfig();
        this.store = store;
        this.memberDir = memberDir;
        this.partitionCount = partitionCount;
    }

    @Override
    public void backup() {
        backup(System.currentTimeMillis());
    }

    @Override
    public void backup(long backupSeq) {
        checkEnabled();
        invokeOnAll(new BackupOperation(backupSeq));
    }

    /** Starts this member's part of the backup; returns once it runs. */
    synchronized void startLocalBackup(long backupSeq) {
        checkEnabled();
        if (state.inProgress()) {
            throw new IllegalStateException("A backup is already in progress");
        }
        Path target = new File(config.getBackupDir(), BACKUP_DIR_PREFIX + backupSeq).toPath().toAbsolutePath()
                .resolve(memberDir.getFileName());
        if (Files.exists(target)) {
            throw new IllegalStateException("Backup " + target + " exists already");
        }
        interrupted = false;
        completed.set(0);
        failed.set(0);
        state = BackupTaskState.IN_PROGRESS;
        logger.info("Backing up the persisted data to " + target);
        try {
            copyMetadata(target);
        } catch (RuntimeException e) {
            state = BackupTaskState.FAILURE;
            throw e;
        }
        OperationService operationService = node.getNodeEngine().getOperationService();
        Path dataTarget = target.resolve(PersistenceModule.DATA);
        for (int p = 0; p < partitionCount; p++) {
            int partitionId = p;
            operationService.execute(new PartitionSpecificRunnable() {
                @Override
                public int getPartitionId() {
                    return partitionId;
                }

                @Override
                public void run() {
                    backupPartition(partitionId, dataTarget);
                }
            });
        }
    }

    private void backupPartition(int partitionId, Path dataTarget) {
        try {
            if (interrupted) {
                failed.incrementAndGet();
            } else {
                store.backupPartition(partitionId, dataTarget);
            }
        } catch (RuntimeException e) {
            logger.warning("Could not back up partition " + partitionId, e);
            failed.incrementAndGet();
        }
        if (completed.incrementAndGet() == partitionCount) {
            state = failed.get() == 0 ? BackupTaskState.SUCCESS : BackupTaskState.FAILURE;
            logger.info("Backup " + (state == BackupTaskState.SUCCESS ? "completed" : "failed"));
        }
    }

    private void copyMetadata(Path target) {
        try {
            Files.createDirectories(target);
            Files.copy(memberDir.resolve(MemberDirectory.MEMBER), target.resolve(MemberDirectory.MEMBER));
            Path dataKey = memberDir.resolve(DataKey.FILE);
            if (Files.exists(dataKey)) {
                Files.copy(dataKey, target.resolve(DataKey.FILE));
            }
            Path cluster = memberDir.resolve(PersistenceModule.CLUSTER);
            Path clusterTarget = Files.createDirectories(target.resolve(PersistenceModule.CLUSTER));
            try (DirectoryStream<Path> files = Files.newDirectoryStream(cluster, file -> !file.toString().endsWith(".tmp"))) {
                for (Path file : files) {
                    Files.copy(file, clusterTarget.resolve(file.getFileName()));
                }
            }
            store.backupStructures(target.resolve(PersistenceModule.DATA));
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot back up to " + target, e);
        }
    }

    @Override
    public BackupTaskStatus getBackupTaskStatus() {
        BackupTaskState current = state;
        if (current == BackupTaskState.NO_TASK) {
            return NO_TASK;
        }
        return new BackupTaskStatus(current, Math.min(completed.get(), partitionCount), partitionCount);
    }

    @Override
    public void interruptLocalBackupTask() {
        interrupted = true;
    }

    @Override
    public void interruptBackupTask() {
        invokeOnAll(new BackupOperation(BackupOperation.INTERRUPT));
    }

    @Override
    public boolean isHotBackupEnabled() {
        return isBackupEnabled();
    }

    @Override
    public boolean isBackupEnabled() {
        return config.getBackupDir() != null;
    }

    @Override
    public String getBackupDirectory() {
        return config.getBackupDir() == null ? null : config.getBackupDir().getAbsolutePath();
    }

    private void checkEnabled() {
        if (!isBackupEnabled()) {
            throw new IllegalStateException("Backups are not enabled: set the persistence backup-dir");
        }
    }

    private void invokeOnAll(BackupOperation operation) {
        OperationService operationService = node.getNodeEngine().getOperationService();
        List<Future<Object>> futures = new ArrayList<>();
        for (Member member : node.getClusterService().getMembers()) {
            futures.add(operationService.invokeOnTarget(InternalHotRestartService.SERVICE_NAME,
                    operation.copy(), member.getAddress()));
        }
        for (Future<Object> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                throw new IllegalStateException("Could not start the backup on all members", e);
            }
        }
    }
}
