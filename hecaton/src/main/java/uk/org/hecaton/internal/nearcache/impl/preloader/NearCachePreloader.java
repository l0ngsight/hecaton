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

package uk.org.hecaton.internal.nearcache.impl.preloader;

import uk.org.hecaton.config.NearCachePreloaderConfig;
import uk.org.hecaton.internal.adapter.DataStructureAdapter;
import uk.org.hecaton.internal.monitor.impl.NearCacheStatsImpl;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.HeapData;
import uk.org.hecaton.internal.util.BufferingInputStream;
import uk.org.hecaton.internal.util.Timer;
import uk.org.hecaton.internal.util.collection.InflatableSet;
import uk.org.hecaton.internal.util.collection.InflatableSet.Builder;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.memory.MemoryUnit;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Iterator;

import static uk.org.hecaton.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static uk.org.hecaton.internal.nio.Bits.readIntB;
import static uk.org.hecaton.internal.nio.Bits.writeIntB;
import static uk.org.hecaton.internal.nio.IOUtil.closeResource;
import static uk.org.hecaton.internal.nio.IOUtil.deleteQuietly;
import static uk.org.hecaton.internal.nio.IOUtil.readFullyOrNothing;
import static uk.org.hecaton.internal.nio.IOUtil.rename;
import static uk.org.hecaton.internal.nio.IOUtil.toFileName;
import static uk.org.hecaton.internal.util.StringUtil.isNullOrEmpty;
import static java.lang.String.format;
import static java.nio.ByteBuffer.allocate;

/**
 * Loads and stores the keys from a Near Cache into a file.
 *
 * @param <K> type of the {@link uk.org.hecaton.internal.nearcache.NearCacheRecord} keys
 */
public class NearCachePreloader<K> {

    /**
     * File format for the file header.
     */
    private enum FileFormat {
        INTERLEAVED_LENGTH_FIELD
    }

    /**
     * Magic bytes for the file header.
     */
    private static final int MAGIC_BYTES = 0xEA3CAC4E;

    /**
     * Base-2 logarithm of buffer size.
     */
    private static final int LOG_OF_BUFFER_SIZE = 16;
    /**
     * Buffer size used for file I/O. Invariant: buffer size is a power of two.
     */
    private static final int BUFFER_SIZE = 1 << LOG_OF_BUFFER_SIZE;

    /**
     * Batch size for the pre-loader.
     */
    private static final int LOAD_BATCH_SIZE = 100;

    private final ILogger logger = Logger.getLogger(NearCachePreloader.class);
    private final byte[] tmpBytes = new byte[INT_SIZE_IN_BYTES];

    private final String nearCacheName;
    private final NearCacheStatsImpl nearCacheStats;
    private final SerializationService serializationService;

    private final NearCachePreloaderLock lock;
    private final File storeFile;
    private final File tmpStoreFile;

    private ByteBuffer buf;
    private int lastWrittenBytes;
    private int lastKeyCount;

    public NearCachePreloader(String nearCacheName, NearCachePreloaderConfig preloaderConfig,
                              NearCacheStatsImpl nearCacheStats, SerializationService serializationService) {
        this.nearCacheName = nearCacheName;
        this.nearCacheStats = nearCacheStats;
        this.serializationService = serializationService;

        String filename = getFilename(preloaderConfig.getDirectory(), nearCacheName);
        this.lock = new NearCachePreloaderLock(logger, filename + ".lock");
        this.storeFile = new File(filename);
        this.tmpStoreFile = new File(filename + "~");
    }

    public void destroy() {
        lock.release();
    }

    /**
     * Loads the values via a stored key file into the supplied {@link DataStructureAdapter}.
     *
     * @param adapter the {@link DataStructureAdapter} to load the values from
     */
    public void loadKeys(DataStructureAdapter<Object, ?> adapter) {
        if (!storeFile.exists()) {
            logger.info(format("Skipped loading keys of Near Cache %s since storage file doesn't exist (%s)", nearCacheName,
                    storeFile.getAbsolutePath()));
            return;
        }

        long startedNanos = Timer.nanos();

        try (BufferingInputStream bis = new BufferingInputStream(new FileInputStream(storeFile), BUFFER_SIZE)) {
            if (!checkHeader(bis)) {
                return;
            }

            int loadedKeys = loadKeySet(bis, adapter);

            long elapsedMillis = Timer.millisElapsed(startedNanos);
            logger.info(format("Loaded %d keys of Near Cache %s in %d ms", loadedKeys, nearCacheName, elapsedMillis));
        } catch (Exception e) {
            logger.warning(format("Could not pre-load Near Cache %s (%s)", nearCacheName, storeFile.getAbsolutePath()), e);
        }
    }

    private boolean checkHeader(BufferingInputStream bis) throws IOException {
        int magicBytes = readInt(bis);
        if (magicBytes != MAGIC_BYTES) {
            logger.warning(format("Found invalid header for Near Cache %s (%s)", nearCacheName, storeFile.getAbsolutePath()));
            return false;
        }
        int fileFormat = readInt(bis);
        if (fileFormat < 0 || fileFormat > FileFormat.values().length - 1) {
            logger.warning(format("Found invalid file format for Near Cache %s (%s)", nearCacheName,
                    storeFile.getAbsolutePath()));
            return false;
        }
        return true;
    }

    /**
     * Stores the Near Cache keys from the supplied iterator.
     *
     * @param iterator {@link Iterator} over the key set of a {@link uk.org.hecaton.internal.nearcache.NearCacheRecordStore}
     */
    public void storeKeys(Iterator<K> iterator) {
        long startedNanos = Timer.nanos();
        FileOutputStream fos = null;
        try {
            buf = allocate(BUFFER_SIZE);
            lastWrittenBytes = 0;
            lastKeyCount = 0;

            fos = new FileOutputStream(tmpStoreFile, false);

            // write header and keys
            writeInt(fos, MAGIC_BYTES);
            writeInt(fos, FileFormat.INTERLEAVED_LENGTH_FIELD.ordinal());
            writeKeySet(fos, fos.getChannel(), iterator);

            // cleanup if no keys have been written
            if (lastKeyCount == 0) {
                deleteQuietly(storeFile);
                updatePersistenceStats(startedNanos);
                return;
            }

            fos.flush();
            closeResource(fos);
            rename(tmpStoreFile, storeFile);

            updatePersistenceStats(startedNanos);
        } catch (Exception e) {
            logger.warning(format("Could not store keys of Near Cache %s (%s)", nearCacheName, storeFile.getAbsolutePath()), e);

            nearCacheStats.addPersistenceFailure(e);
        } finally {
            closeResource(fos);
            deleteQuietly(tmpStoreFile);
        }
    }

    private void updatePersistenceStats(long startedNanos) {
        long elapsedMillis = Timer.millisElapsed(startedNanos);
        nearCacheStats.addPersistence(elapsedMillis, lastWrittenBytes, lastKeyCount);

        logger.info(format("Stored %d keys of Near Cache %s in %d ms (%d kB)", lastKeyCount, nearCacheName, elapsedMillis,
                MemoryUnit.BYTES.toKiloBytes(lastWrittenBytes)));
    }

    private int loadKeySet(BufferingInputStream bis, DataStructureAdapter<Object, ?> adapter) throws IOException {
        int loadedKeys = 0;

        Builder<Object> builder = InflatableSet.newBuilder(LOAD_BATCH_SIZE);
        while (readFullyOrNothing(bis, tmpBytes)) {
            int dataSize = readIntB(tmpBytes, 0);
            byte[] payload = new byte[dataSize];
            if (!readFullyOrNothing(bis, payload)) {
                break;
            }
            Data key = new HeapData(payload);
            builder.add(serializationService.toObject(key));
            if (builder.size() == LOAD_BATCH_SIZE) {
                adapter.getAll(builder.build());
                builder = InflatableSet.newBuilder(LOAD_BATCH_SIZE);
            }
            loadedKeys++;
        }
        if (builder.size() > 0) {
            adapter.getAll(builder.build());
        }
        return loadedKeys;
    }

    private void writeKeySet(FileOutputStream fos, FileChannel outChannel, Iterator<K> iterator) throws IOException {
        while (iterator.hasNext()) {
            K key = iterator.next();
            Data dataKey = serializationService.toData(key);
            if (dataKey != null) {
                int dataSize = dataKey.totalSize();
                writeInt(fos, dataSize);

                int position = 0;
                int remaining = dataSize;
                while (remaining > 0) {
                    int transferredCount = Math.min(BUFFER_SIZE - buf.position(), remaining);
                    ensureBufHasRoom(fos, transferredCount);
                    buf.put(dataKey.toByteArray(), position, transferredCount);
                    position += transferredCount;
                    remaining -= transferredCount;
                }

                lastWrittenBytes += INT_SIZE_IN_BYTES + dataSize;
                lastKeyCount++;
            }
            flushLocalBuffer(outChannel);
        }
    }

    private int readInt(BufferingInputStream bis) throws IOException {
        readFullyOrNothing(bis, tmpBytes);
        return readIntB(tmpBytes, 0);
    }

    private void writeInt(FileOutputStream fos, int dataSize) throws IOException {
        ensureBufHasRoom(fos, INT_SIZE_IN_BYTES);
        writeIntB(tmpBytes, 0, dataSize);
        buf.put(tmpBytes);
    }

    private void ensureBufHasRoom(FileOutputStream fos, int expectedSize) throws IOException {
        if (buf.position() < BUFFER_SIZE - expectedSize) {
            return;
        }
        fos.write(buf.array());
        buf.position(0);
    }

    private void flushLocalBuffer(FileChannel outChannel) throws IOException {
        if (buf.position() == 0) {
            return;
        }
        buf.flip();
        while (buf.hasRemaining()) {
            outChannel.write(buf);
        }
        buf.clear();
    }

    private static String getFilename(String directory, String nearCacheName) {
        String filename = toFileName("nearCache-" + nearCacheName + ".store");
        if (isNullOrEmpty(directory)) {
            return filename;
        }
        return Paths.get(directory, filename).toString();
    }
}
