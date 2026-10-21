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

import uk.org.hecaton.internal.memory.MemoryAllocator;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.impl.HeapData;

/**
 * Serialized {@link Data} in a native block: its length as an int, then its bytes.
 */
public final class NativeData {

    private static final int LENGTH = Integer.BYTES;
    /** Where the bytes that {@link Data#equals} compares start: the partition hash is not compared. */
    private static final int TYPE_OFFSET = HeapData.TYPE_OFFSET;

    private NativeData() {
    }

    /** Copies {@code data} into a new block. */
    public static long write(MemoryAllocator memory, Data data) {
        byte[] bytes = data.toByteArray();
        int length = bytes == null ? 0 : bytes.length;
        long address = memory.allocate(LENGTH + length);
        NativeAccess.putInt(address, length);
        if (length > 0) {
            NativeAccess.copyFromArray(bytes, 0, address + LENGTH, length);
        }
        return address;
    }

    /** A heap copy of the data in the block. */
    public static HeapData read(long address) {
        int length = NativeAccess.getInt(address);
        byte[] bytes = new byte[length];
        NativeAccess.copyToArray(address + LENGTH, bytes, 0, length);
        return new HeapData(bytes);
    }

    /** The size of the block. */
    public static long blockSize(long address) {
        return LENGTH + NativeAccess.getInt(address);
    }

    public static void free(MemoryAllocator memory, long address) {
        memory.free(address, blockSize(address));
    }

    /** Whether the block holds data equal to {@code data}, as {@link Data#equals} defines it. */
    public static boolean equals(long address, Data data) {
        byte[] bytes = data.toByteArray();
        int length = bytes == null ? 0 : bytes.length;
        if (NativeAccess.getInt(address) != length) {
            return false;
        }
        if (length <= TYPE_OFFSET) {
            return true;
        }
        return NativeAccess.equals(address + LENGTH + TYPE_OFFSET, bytes, TYPE_OFFSET, length - TYPE_OFFSET);
    }
}
