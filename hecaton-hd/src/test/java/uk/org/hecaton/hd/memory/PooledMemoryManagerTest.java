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

import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PooledMemoryManagerTest {

    private static final int PAGE = 1 << 12;

    private final PooledMemoryManager memory = new PooledMemoryManager(16 << 20, 1 << 20, PAGE, 16);

    @After
    public void tearDown() {
        memory.dispose();
    }

    @Test
    public void sizesRoundUpToPowersOfTwo() {
        assertThat(memory.orderOf(1)).isZero();
        assertThat(memory.orderOf(16)).isZero();
        assertThat(memory.orderOf(17)).isEqualTo(1);
        assertThat(memory.orderOf(32)).isEqualTo(1);
        assertThat(memory.orderOf(PAGE)).isEqualTo(8);
        long address = memory.allocate(17);
        assertThat(memory.usedNative()).isEqualTo(32);
        memory.free(address, 17);
    }

    @Test
    public void blocksAreAlignedAndDistinct() {
        Set<Long> addresses = new HashSet<>();
        for (int i = 0; i < PAGE / 64; i++) {
            long address = memory.allocate(64);
            assertThat(address % 64).isZero();
            assertThat(addresses.add(address)).isTrue();
        }
        // exactly one page
        assertThat(memory.pageCount()).isEqualTo(1);
        for (long address : addresses) {
            memory.free(address, 64);
        }
    }

    @Test
    public void buddiesMergeAndPagesAreReturned() {
        List<Long> blocks = new ArrayList<>();
        for (int i = 0; i < 10 * PAGE / 16; i++) {
            blocks.add(memory.allocate(16));
        }
        assertThat(memory.pageCount()).isEqualTo(10);
        for (long block : blocks) {
            memory.free(block, 16);
        }
        // merged back to whole pages; one free page is kept
        assertThat(memory.pageCount()).isEqualTo(1);
        assertThat(memory.usedNative()).isZero();
        // and the kept page serves a whole-page block
        long page = memory.allocate(PAGE);
        assertThat(memory.pageCount()).isEqualTo(1);
        memory.free(page, PAGE);
    }

    @Test
    public void largeBlocksComeFromTheCLibrary() {
        long address = memory.allocate(3 * PAGE);
        assertThat(memory.pageCount()).isZero();
        assertThat(memory.usedNative()).isEqualTo(3 * PAGE);
        memory.free(address, 3 * PAGE);
        assertThat(memory.committedNative()).isZero();
    }
}
