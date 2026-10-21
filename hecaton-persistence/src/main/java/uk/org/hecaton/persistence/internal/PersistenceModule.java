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

import uk.org.hecaton.cache.impl.CacheRecordObserver;
import uk.org.hecaton.cache.impl.ICacheRecordStore;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.PersistenceConfig;
import uk.org.hecaton.hotrestart.HotRestartService;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.module.MemberModule;
import uk.org.hecaton.instance.module.PersistenceCapability;
import uk.org.hecaton.internal.hotrestart.InternalHotRestartService;
import uk.org.hecaton.internal.hotrestart.NoopInternalHotRestartService;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.MutationObserver;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.persistence.internal.cache.CachePersistenceObserver;
import uk.org.hecaton.persistence.internal.cluster.ClusterMetadata;
import uk.org.hecaton.persistence.internal.cluster.RecoveryService;
import uk.org.hecaton.persistence.internal.map.MapPersistenceObserver;
import uk.org.hecaton.persistence.internal.store.LogSettings;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.spi.properties.HecatonProperty;
import uk.org.hecaton.version.Version;

import java.nio.file.Path;
import java.util.UUID;

/**
 * Persistence (F10): IMap data and cluster metadata on disk, and the recovery of a restarted
 * cluster (see {@code docs/design/persistence.md}).
 */
public class PersistenceModule implements MemberModule, PersistenceCapability {

    /** Size at which a partition's active chunk file is closed and a new one started. */
    public static final HecatonProperty MAX_CHUNK_SIZE =
            new HecatonProperty("hecaton.persistence.max.chunk.size", LogSettings.DEFAULT.maxChunkSize());

    static final String DATA = "data";
    static final String CLUSTER = "cluster";

    private final InternalHotRestartService notStarted = new NoopInternalHotRestartService();
    private Node node;
    private MemberDirectory directory;
    private DataStore dataStore;
    private RecoveryService recovery;
    private BackupService backup;

    @Override
    public String name() {
        return "persistence";
    }

    @Override
    public UUID start(Node node, UUID newUuid) {
        this.node = node;
        PersistenceConfig config = node.getConfig().getPersistenceConfig();
        if (config.getEncryptionAtRestConfig().isEnabled()) {
            throw new IllegalStateException("Encryption at rest (F10) is not available in Hecaton yet; it is planned for "
                    + "milestone M9. Disable persistence.encryption-at-rest.");
        }
        directory = MemberDirectory.claim(config.getBaseDir().toPath().toAbsolutePath());
        UUID uuid = directory.memberUuid();
        if (uuid == null) {
            uuid = newUuid;
            directory.writeMemberUuid(uuid);
        }
        Path dir = directory.path();
        int partitionCount = node.getProperties().getInteger(ClusterProperty.PARTITION_COUNT);
        LogSettings settings = new LogSettings(node.getProperties().getLong(MAX_CHUNK_SIZE),
                LogSettings.DEFAULT.compactionGarbageRatio());
        dataStore = new DataStore(dir.resolve(DATA), partitionCount, settings);
        recovery = new RecoveryService(node, new ClusterMetadata(dir.resolve(CLUSTER)),
                new LocalPartitionData(dataStore, node::getNodeEngine), uuid);
        backup = new BackupService(node, dataStore, dir, partitionCount);
        return uuid;
    }

    @Override
    public void init(Node node) {
        // started by the node extension when persistence is enabled; otherwise the module is idle
        if (directory != null) {
            node.getLogger(PersistenceModule.class).info("Persistence directory: " + directory.path());
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        if (dataStore != null) {
            dataStore.close();
        }
        if (directory != null) {
            directory.close();
        }
    }

    @Override
    public InternalHotRestartService internalHotRestartService() {
        return recovery == null ? notStarted : recovery;
    }

    @Override
    public HotRestartService hotRestartService() {
        return backup;
    }

    @Override
    public MutationObserver<Record> createMapObserver(RecordStore<Record> recordStore) {
        return new MapPersistenceObserver(dataStore, recordStore, node.getSerializationService());
    }

    @Override
    public CacheRecordObserver createCacheObserver(ICacheRecordStore recordStore) {
        return new CachePersistenceObserver(dataStore, recordStore, node.getSerializationService());
    }

    @Override
    public void beforeJoin() {
        // recovery starts once the member has joined
    }

    @Override
    public void afterStart() {
        recovery.afterStart();
    }

    @Override
    public void onMemberListChange() {
        recovery.onMemberListChange();
    }

    @Override
    public void onPartitionStateChange() {
        recovery.onPartitionStateChange();
    }

    @Override
    public void onClusterStateChange(ClusterState newState) {
        recovery.onClusterStateChange(newState);
    }

    @Override
    public void onClusterVersionChange(Version newVersion) {
        recovery.onClusterVersionChange(newVersion);
    }
}
