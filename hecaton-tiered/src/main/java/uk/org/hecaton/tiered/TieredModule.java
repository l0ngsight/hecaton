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

package uk.org.hecaton.tiered;

import uk.org.hecaton.config.DeviceConfig;
import uk.org.hecaton.config.LocalDeviceConfig;
import uk.org.hecaton.config.TieredStoreConfig;
import uk.org.hecaton.hd.HdModule;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.module.MemberModule;
import uk.org.hecaton.instance.impl.module.NativeMemoryCapability;
import uk.org.hecaton.instance.impl.module.TieredStoreCapability;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapKeyLoader;
import uk.org.hecaton.map.impl.recordstore.RecordStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * The Tiered Storage module ({@code hecaton-tiered}): maps with a tiered store keep their values in
 * native memory (from {@code hecaton-hd}) up to the memory tier's capacity, and the rest in files on
 * the disk tier's device, under {@code <base-dir>/hecaton-tiered/<member UUID>}. The disk tier is
 * not persistent: a member deletes its directory when it shuts down, and the directories of members
 * that are gone when it starts.
 */
public class TieredModule implements MemberModule, TieredStoreCapability {

    private static final String DIRECTORY = "hecaton-tiered";
    private static final String LOCK_FILE = "lock";

    private final Map<String, MemoryTierBudget> budgets = new ConcurrentHashMap<>();
    private final Map<String, MemberDirectory> directories = new ConcurrentHashMap<>();
    private volatile Node node;

    @Override
    public String name() {
        return "tiered";
    }

    @Override
    public void init(Node node) {
        this.node = node;
    }

    @Override
    public void shutdown(boolean terminate) {
        directories.values().forEach(MemberDirectory::delete);
        directories.clear();
    }

    @Override
    public RecordStore createMapRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader,
                                            ILogger logger) {
        return new TieredRecordStore(mapContainer, partitionId, keyLoader, logger, this);
    }

    /** The memory tier's use of a map on this member. */
    MemoryTierBudget budget(String mapName) {
        return budgets.get(mapName);
    }

    PartitionTiers tiers(MapContainer mapContainer, int partitionId) {
        String mapName = mapContainer.getName();
        TieredStoreConfig config = mapContainer.getMapConfig().getTieredStoreConfig();
        HdModule hd = (HdModule) node.getMemberModules().require(NativeMemoryCapability.class, "Tiered Storage");
        MemoryTierBudget budget = budgets.computeIfAbsent(mapName,
                name -> new MemoryTierBudget(config.getMemoryTierConfig().getCapacity().bytes()));
        String deviceName = config.getDiskTierConfig().getDeviceName();
        DeviceConfig device = node.getConfig().getDeviceConfig(deviceName);
        if (!(device instanceof LocalDeviceConfig local)) {
            throw new IllegalStateException("The disk tier of map " + mapName + " needs a local device; " + deviceName
                    + " is not one");
        }
        Path mapDirectory = directories.computeIfAbsent(local.getName(), name -> claim(local.getBaseDir().toPath()))
                .path().resolve("map-" + HexFormat.of().formatHex(mapName.getBytes(StandardCharsets.UTF_8)));
        return new PartitionTiers(hd.memoryManager(), budget, mapDirectory, partitionId);
    }

    /** Takes a directory for this member under the device's base directory, deleting those of members that are gone. */
    private MemberDirectory claim(Path baseDir) {
        Path root = baseDir.toAbsolutePath().resolve(DIRECTORY);
        try {
            Files.createDirectories(root);
            try (Stream<Path> others = Files.list(root)) {
                for (Path other : others.toList()) {
                    deleteIfUnlocked(other);
                }
            }
            Path own = root.resolve(node.getThisUuid().toString());
            Files.createDirectories(own);
            FileChannel channel = FileChannel.open(own.resolve(LOCK_FILE), StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
            return new MemberDirectory(own, channel, channel.lock());
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create the disk tier directory under " + root, e);
        }
    }

    private static void deleteIfUnlocked(Path directory) throws IOException {
        Path lockFile = directory.resolve(LOCK_FILE);
        if (!Files.isDirectory(directory)) {
            return;
        }
        if (Files.exists(lockFile)) {
            try (FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.WRITE)) {
                FileLock lock = channel.tryLock();
                if (lock == null) {
                    return;
                }
                lock.release();
            } catch (OverlappingFileLockException e) {
                // a member in this JVM uses it
                return;
            }
        }
        deleteRecursively(directory);
    }

    static void deleteRecursively(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            List<Path> all = paths.sorted(Comparator.reverseOrder()).toList();
            for (Path path : all) {
                Files.deleteIfExists(path);
            }
        }
    }

    /** A member's directory on a device, locked while the member runs. */
    private record MemberDirectory(Path path, FileChannel channel, FileLock lock) {

        void delete() {
            try {
                lock.release();
                channel.close();
                deleteRecursively(path);
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot delete the disk tier directory " + path, e);
            }
        }
    }
}
