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

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * The C library's allocation functions.
 */
final class Libc {

    private static final MethodHandle MALLOC;
    private static final MethodHandle ALIGNED_ALLOC;
    private static final MethodHandle FREE;

    static {
        Linker linker = Linker.nativeLinker();
        SymbolLookup libc = linker.defaultLookup();
        MALLOC = linker.downcallHandle(libc.find("malloc").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
        ALIGNED_ALLOC = libc.find("aligned_alloc").map(symbol -> linker.downcallHandle(symbol,
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)))
                .orElse(null);
        FREE = linker.downcallHandle(libc.find("free").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    }

    private Libc() {
    }

    /** @return the address, or 0 if the C library has no memory */
    static long malloc(long size) {
        try {
            return ((MemorySegment) MALLOC.invokeExact(size)).address();
        } catch (Throwable t) {
            throw new IllegalStateException(t);
        }
    }

    /**
     * @param alignment a power of two, a multiple of the pointer size
     * @param size      a multiple of {@code alignment}
     * @return the address, or 0 if the C library has no memory
     */
    static long alignedAlloc(long alignment, long size) {
        if (ALIGNED_ALLOC == null) {
            throw new UnsupportedOperationException("The C library has no aligned_alloc; use the STANDARD allocator");
        }
        try {
            return ((MemorySegment) ALIGNED_ALLOC.invokeExact(alignment, size)).address();
        } catch (Throwable t) {
            throw new IllegalStateException(t);
        }
    }

    static void free(long address) {
        try {
            FREE.invokeExact(MemorySegment.ofAddress(address));
        } catch (Throwable t) {
            throw new IllegalStateException(t);
        }
    }
}
