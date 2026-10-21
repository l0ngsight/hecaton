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
package uk.org.hecaton.hd.table;

import uk.org.hecaton.internal.iteration.IterationPointer;

/**
 * Converts between the {@link IterationPointer}s that clients keep between fetches and
 * {@link NativeHashTable.Cursor}s. A cursor's hash is split between the pointer's index (its high
 * 31 bits) and size (its low bit, above the 30-bit count of entries with that hash), so both stay
 * non-negative: the start pointer has size -1 and the end pointer index -1.
 */
public final class IterationPointers {

    private static final int COUNT_BITS = 30;
    private static final int COUNT_MASK = (1 << COUNT_BITS) - 1;

    private IterationPointers() {
    }

    public static NativeHashTable.Cursor toCursor(IterationPointer[] pointers) {
        IterationPointer pointer = pointers[pointers.length - 1];
        if (pointer.getSize() < 0 || pointer.getIndex() < 0) {
            return new NativeHashTable.Cursor();
        }
        int hash = (pointer.getIndex() << 1) | (pointer.getSize() >>> COUNT_BITS);
        return new NativeHashTable.Cursor(hash, pointer.getSize() & COUNT_MASK);
    }

    public static IterationPointer[] fromCursor(NativeHashTable.Cursor cursor, boolean done) {
        if (done) {
            return new IterationPointer[]{new IterationPointer(-1, -1)};
        }
        if (!cursor.started()) {
            return new IterationPointer[]{new IterationPointer(Integer.MAX_VALUE, -1)};
        }
        int hash = cursor.hash();
        int index = hash >>> 1;
        int size = ((hash & 1) << COUNT_BITS) | Math.min(cursor.skip(), COUNT_MASK);
        return new IterationPointer[]{new IterationPointer(index, size)};
    }
}
