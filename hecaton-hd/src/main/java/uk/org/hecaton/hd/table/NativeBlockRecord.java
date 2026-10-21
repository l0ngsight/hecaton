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

import uk.org.hecaton.hd.memory.HdMemoryManager;

/**
 * A record kept in a native record block by a {@link NativeRecordTable}: either a view of the
 * block (attached) or a heap object (detached: not stored yet, or removed). A record block starts
 * with the address of its key block.
 */
public interface NativeBlockRecord {

    /** The record block, or 0 if detached. */
    long address();

    /**
     * Writes this detached record into a new record block for the key block and makes this object
     * a view of it. On failure nothing is allocated and the record stays detached.
     *
     * @return the record block
     */
    long attach(HdMemoryManager memory, long keyBlock);

    /** Copies the record block's state into this object, which then no longer refers to it. */
    void detach();
}
