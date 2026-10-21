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

import uk.org.hecaton.persistence.internal.store.AtomicFiles;
import uk.org.hecaton.persistence.internal.store.LogSettings;
import uk.org.hecaton.persistence.internal.store.PartitionLog;
import uk.org.hecaton.persistence.internal.store.PersistenceStoreException;
import uk.org.hecaton.persistence.internal.store.StructureRegistry;
import uk.org.hecaton.persistence.internal.store.StructureRegistry.Structure;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * The data of a member: one {@link PartitionLog} per partition and the ids of the structures. A
 * partition's log is used by the partition's thread only.
 */
public final class DataStore implements AutoCloseable {

    private static final String PARTITION_PREFIX = "p";

    private final Path dir;
    private final LogSettings settings;
    private final StructureRegistry structures;
    private final AtomicReferenceArray<PartitionLog> logs;
    /** 1 while a partition is being loaded: its writes come from the log itself and are not logged again. */
    private final AtomicIntegerArray loading;
    private volatile boolean closed;

    DataStore(Path dir, int partitionCount, LogSettings settings) {
        this.dir = dir;
        this.settings = settings;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot create " + dir, e);
        }
        this.structures = new StructureRegistry(dir.resolve("structures"));
        this.logs = new AtomicReferenceArray<>(partitionCount);
        this.loading = new AtomicIntegerArray(partitionCount);
    }

    /** The log of the partition, opened if needed. */
    public PartitionLog log(int partitionId) {
        PartitionLog log = logs.get(partitionId);
        if (log == null) {
            if (closed) {
                throw new PersistenceStoreException("The data store in " + dir + " is closed");
            }
            log = PartitionLog.open(dir.resolve(PARTITION_PREFIX + partitionId), settings);
            logs.set(partitionId, log);
        }
        return log;
    }

    public int structureId(byte kind, String name) {
        return structures.idOf(kind, name);
    }

    /**
     * Keeps the configuration of a structure, so it can be recreated when the data is loaded (for
     * caches created at runtime).
     */
    public void saveConfig(int structureId, byte[] config) {
        Path file = configFile(structureId);
        if (!Files.exists(file)) {
            AtomicFiles.write(file, config);
        }
    }

    /** @return the saved configuration of the structure, or null */
    public byte[] config(int structureId) {
        return AtomicFiles.read(configFile(structureId));
    }

    private Path configFile(int structureId) {
        Path configs = dir.resolve("configs");
        try {
            Files.createDirectories(configs);
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot create " + configs, e);
        }
        return configs.resolve(Integer.toString(structureId));
    }

    /** @return the structure, or null if the id is unknown */
    public Structure structure(int id) {
        return structures.get(id);
    }

    public boolean isLoading(int partitionId) {
        return loading.get(partitionId) != 0;
    }

    void setLoading(int partitionId, boolean value) {
        loading.set(partitionId, value ? 1 : 0);
    }

    /** Deletes the partition's log. */
    void deletePartition(int partitionId) {
        PartitionLog log = logs.getAndSet(partitionId, null);
        if (log == null) {
            log = PartitionLog.open(dir.resolve(PARTITION_PREFIX + partitionId), settings);
        }
        log.delete();
    }

    /** The partitions that have a log on disk. */
    Set<Integer> partitionsOnDisk() {
        Set<Integer> partitions = new TreeSet<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, PARTITION_PREFIX + "*")) {
            for (Path file : files) {
                try {
                    partitions.add(Integer.parseInt(file.getFileName().toString().substring(PARTITION_PREFIX.length())));
                } catch (NumberFormatException ignored) {
                    // not a log
                }
            }
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot list " + dir, e);
        }
        return partitions;
    }

    @Override
    public void close() {
        closed = true;
        for (int i = 0; i < logs.length(); i++) {
            PartitionLog log = logs.getAndSet(i, null);
            if (log != null) {
                log.close();
            }
        }
        structures.close();
    }
}
