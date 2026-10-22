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

package uk.org.hecaton.map.impl.operation;

/**
 * How an operation uses an immutable map (load protection, F23): immutable maps are insert-only,
 * and {@link MapOperation} checks the operation's access on the partition owner before it runs.
 */
public enum ImmutableMapAccess {

    /** Does not change the map's entries, or checks the rules itself. Always allowed. */
    READ,

    /** Inserts its key: allowed when the map does not hold the key. */
    INSERT,

    /** Changes or removes entries: refused. */
    CHANGE
}
