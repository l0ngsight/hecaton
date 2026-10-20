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

import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import static uk.org.hecaton.internal.nio.IOUtil.closeResource;
import static uk.org.hecaton.internal.nio.IOUtil.deleteQuietly;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NearCachePreloaderLockTest extends HecatonTestSupport {


    private final ILogger logger = mock(ILogger.class);
    private final File preloaderLockFile = new File(randomName());
    private final File lockFile = new File(randomName());

    private NearCachePreloaderLock preloaderLock;
    private FileChannel channel;
    private FileChannel anotherChannel;

    @Before
    public void setUp() throws Exception {
        preloaderLock = new NearCachePreloaderLock(logger, preloaderLockFile.getAbsolutePath());

        channel = new RandomAccessFile(lockFile, "rw").getChannel();
        anotherChannel = new RandomAccessFile(lockFile, "rw").getChannel();
    }

    @After
    public void tearDown() {
        closeResource(channel);
        closeResource(anotherChannel);
        preloaderLock.release();
        deleteQuietly(lockFile);
        deleteQuietly(preloaderLockFile);
    }

    /**
     * The {@link FileChannel#tryLock()} returns:
     * <pre>
     * A lock object representing the newly-acquired lock,
     * or <tt>null</tt> if the lock could not be acquired
     * because another program holds an overlapping lock
     * </pre>
     */
    @Test
    public void testAcquireLock_whenTryLockReturnsNull_thenThrowHecatonException() throws IOException {
        try (NotLockingDummyFileChannel notLockingDummyFileChannel = new NotLockingDummyFileChannel()) {
            assertThatThrownBy(() -> preloaderLock.acquireLock(lockFile, notLockingDummyFileChannel))
                    .isInstanceOf(HecatonException.class)
                    .hasMessageContaining("File is already being used by another Hecaton instance.");
        }
    }

    /**
     * The {@link FileChannel#tryLock()} throws an {@link OverlappingFileLockException}:
     * <pre>
     * If a lock that overlaps the requested region is already held by
     * this Java virtual machine, or if another thread is already
     * blocked in this method and is attempting to lock an overlapping
     * region
     * </pre>
     */
    @Test
    public void testAcquireLock_whenTryLockThrowsOverlappingFileLockException_thenThrowHecatonException() throws Exception {
        try (FileLock anotherLock = anotherChannel.tryLock()) {
            assertThat(anotherLock).isNotNull();

            assertThatThrownBy(() -> preloaderLock.acquireLock(lockFile, channel))
                    .isInstanceOf(HecatonException.class)
                    .hasMessageContaining("File is already being used by this Hecaton instance.");
        }
    }

    /**
     * The {@link FileChannel#tryLock()} throws an {@link IOException}:
     * <pre>
     * If some other I/O error occurs
     * </pre>
     */
    @Test
    public void testAcquireLock_whenTryLockThrowsIOException_thenThrowHecatonException() throws Exception {
        channel.close(); //locking on close channel will result with IOException

        assertThatThrownBy(() -> preloaderLock.acquireLock(lockFile, channel))
                .isInstanceOf(HecatonException.class)
                .hasMessageContaining("Unknown failure while acquiring lock on " + lockFile.getAbsolutePath());
    }

    @Test
    public void testRelease() {
        try {
            preloaderLock.release();
        } catch (Throwable e) {
            e.printStackTrace();
            fail("Cannot release preloaderLock");
        }
    }

    private static class NotLockingDummyFileChannel extends FileChannel {

        @Override
        public int read(ByteBuffer dst) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) {
            throw new UnsupportedOperationException();

        }

        @Override
        public int write(ByteBuffer src) {
            throw new UnsupportedOperationException();

        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) {
            throw new UnsupportedOperationException();

        }

        @Override
        public long position() {
            throw new UnsupportedOperationException();

        }

        @Override
        public FileChannel position(long newPosition) {
            throw new UnsupportedOperationException();

        }

        @Override
        public long size() {
            throw new UnsupportedOperationException();

        }

        @Override
        public FileChannel truncate(long size) {
            throw new UnsupportedOperationException();

        }

        @Override
        public void force(boolean metaData) {
            throw new UnsupportedOperationException();

        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) {
            throw new UnsupportedOperationException();

        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) {
            throw new UnsupportedOperationException();

        }

        @Override
        public int read(ByteBuffer dst, long position) {
            throw new UnsupportedOperationException();

        }

        @Override
        public int write(ByteBuffer src, long position) {
            throw new UnsupportedOperationException();

        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) {
            throw new UnsupportedOperationException();

        }

        @Override
        public FileLock lock(long position, long size, boolean shared) {
            throw new UnsupportedOperationException();

        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) {
            return null;
        }

        @Override
        protected void implCloseChannel() {
            //no-op to make close() working
        }
    }

}
