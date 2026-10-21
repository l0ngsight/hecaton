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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Reads and writes native memory by address. Addresses come from a {@link HdMemoryManager}; the
 * caller is responsible for staying within the blocks it allocated.
 */
public final class NativeAccess {

    /** All of the address space, so an address can be used as an offset. */
    private static final MemorySegment ALL = MemorySegment.NULL.reinterpret(Long.MAX_VALUE);
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT_UNALIGNED;
    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED;

    private NativeAccess() {
    }

    public static byte getByte(long address) {
        return ALL.get(ValueLayout.JAVA_BYTE, address);
    }

    public static void putByte(long address, byte value) {
        ALL.set(ValueLayout.JAVA_BYTE, address, value);
    }

    public static int getInt(long address) {
        return ALL.get(INT, address);
    }

    public static void putInt(long address, int value) {
        ALL.set(INT, address, value);
    }

    public static long getLong(long address) {
        return ALL.get(LONG, address);
    }

    public static void putLong(long address, long value) {
        ALL.set(LONG, address, value);
    }

    public static void copyFromArray(byte[] source, int offset, long address, int length) {
        MemorySegment.copy(source, offset, ALL, ValueLayout.JAVA_BYTE, address, length);
    }

    public static void copyToArray(long address, byte[] target, int offset, int length) {
        MemorySegment.copy(ALL, ValueLayout.JAVA_BYTE, address, target, offset, length);
    }

    public static void copy(long source, long target, long length) {
        MemorySegment.copy(ALL, source, ALL, target, length);
    }

    public static void fill(long address, long length, byte value) {
        ALL.asSlice(address, length).fill(value);
    }

    /** Whether {@code length} bytes at {@code address} equal {@code bytes}. */
    public static boolean equals(long address, byte[] bytes, int length) {
        return MemorySegment.mismatch(ALL, address, address + length,
                MemorySegment.ofArray(bytes), 0, length) == -1;
    }
}
