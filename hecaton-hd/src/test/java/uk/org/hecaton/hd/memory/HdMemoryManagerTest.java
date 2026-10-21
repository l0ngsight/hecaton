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
package uk.org.hecaton.hd.memory;

import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.config.NativeMemoryConfig.MemoryAllocatorType;
import uk.org.hecaton.memory.Capacity;
import uk.org.hecaton.memory.MemoryUnit;
import uk.org.hecaton.memory.NativeOutOfMemoryError;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HdMemoryManagerTest {

    private static final int PAGE_SIZE = 1 << 16;

    @Parameterized.Parameter
    public MemoryAllocatorType allocatorType;

    private HdMemoryManager memory;

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return MemoryAllocatorType.values();
    }

    @Before
    public void setUp() {
        memory = create(64);
    }

    @After
    public void tearDown() {
        memory.dispose();
    }

    private HdMemoryManager create(long megabytes) {
        NativeMemoryConfig config = new NativeMemoryConfig().setEnabled(true)
                .setAllocatorType(allocatorType)
                .setCapacity(new Capacity(megabytes, MemoryUnit.MEGABYTES))
                .setPageSize(PAGE_SIZE)
                .setMinBlockSize(16)
                .setMetadataSpacePercentage(10);
        return HdMemoryManager.create(config);
    }

    @Test
    public void blocksHoldTheirContents() {
        Random random = new Random(1);
        List<long[]> blocks = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            long size = 1 + random.nextInt(i % 10 == 0 ? 3 * PAGE_SIZE : 600);
            long address = memory.allocate(size);
            byte pattern = (byte) i;
            NativeAccess.fill(address, size, pattern);
            blocks.add(new long[]{address, size, pattern});
            if (random.nextInt(3) == 0) {
                long[] victim = blocks.remove(random.nextInt(blocks.size()));
                verify(victim);
                memory.free(victim[0], victim[1]);
            }
        }
        for (long[] block : blocks) {
            verify(block);
            memory.free(block[0], block[1]);
        }
        assertThat(memory.usedNative()).isZero();
    }

    @Test
    public void usedMemoryIsAccounted() {
        long address = memory.allocate(100);
        assertThat(memory.usedNative()).isGreaterThanOrEqualTo(100);
        assertThat(memory.committedNative()).isGreaterThanOrEqualTo(memory.usedNative());
        memory.free(address, 100);
        assertThat(memory.usedNative()).isZero();
    }

    @Test
    public void allocationBeyondCapacityFails() {
        HdMemoryManager small = create(1);
        try {
            List<Long> blocks = new ArrayList<>();
            assertThatThrownBy(() -> {
                while (true) {
                    blocks.add(small.allocate(4096));
                }
            }).isInstanceOf(NativeOutOfMemoryError.class);
            assertThat(small.committedNative() + small.usedMetadata()).isLessThanOrEqualTo(1 << 20);
            for (long block : blocks) {
                small.free(block, 4096);
            }
            // the memory is usable again
            small.free(small.allocate(4096), 4096);
        } finally {
            small.dispose();
        }
    }

    @Test
    public void metadataIsLimitedToItsShare() {
        long max = memory.maxMetadata();
        assertThat(max).isEqualTo((64L << 20) / 10);
        assertThatThrownBy(() -> memory.allocateMetadata(max + 1)).isInstanceOf(NativeOutOfMemoryError.class);
        long address = memory.allocateMetadata(1024);
        assertThat(memory.usedMetadata()).isEqualTo(1024);
        memory.freeMetadata(address, 1024);
        assertThat(memory.usedMetadata()).isZero();
    }

    @Test
    public void reallocateKeepsContents() {
        long address = memory.allocate(40);
        NativeAccess.fill(address, 40, (byte) 7);
        long moved = memory.reallocate(address, 40, 5000);
        for (int i = 0; i < 40; i++) {
            assertThat(NativeAccess.getByte(moved + i)).isEqualTo((byte) 7);
        }
        memory.free(moved, 5000);
        assertThat(memory.usedNative()).isZero();
    }

    @Test
    public void threadsFreeEachOthersBlocks() throws Exception {
        ConcurrentLinkedQueue<long[]> handOver = new ConcurrentLinkedQueue<>();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < 4; t++) {
                int seed = t;
                futures.add(executor.submit(() -> {
                    Random random = new Random(seed);
                    for (int i = 0; i < 20_000; i++) {
                        long size = 1 + random.nextInt(300);
                        long address = memory.allocate(size);
                        NativeAccess.putByte(address, (byte) size);
                        handOver.add(new long[]{address, size});
                        long[] other = handOver.poll();
                        if (other != null) {
                            assertThat(NativeAccess.getByte(other[0])).isEqualTo((byte) other[1]);
                            memory.free(other[0], other[1]);
                        }
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }
        for (long[] block : handOver) {
            memory.free(block[0], block[1]);
        }
        assertThat(memory.usedNative()).isZero();
    }

    private static void verify(long[] block) {
        for (long offset = 0; offset < block[1]; offset += 97) {
            assertThat(NativeAccess.getByte(block[0] + offset)).isEqualTo((byte) block[2]);
        }
    }
}
