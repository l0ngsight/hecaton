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
package uk.org.hecaton.instance.module;

import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.MutationObserver;
import uk.org.hecaton.map.impl.recordstore.RecordStore;

import javax.annotation.Nullable;

/**
 * Vector collections (F20), provided by {@code uk.org.hecaton:hecaton-vector}. A collection's documents are
 * stored in an IMap named {@link #BACKING_MAP_PREFIX} + the collection name; the module keeps the vector
 * indexes of each partition up to date through an observer of the map's record stores.
 */
public non-sealed interface VectorCapability extends MemberCapability {

    /** The prefix of the names of the IMaps that store the documents of vector collections. */
    String BACKING_MAP_PREFIX = "__vector.";

    /**
     * The observer that keeps the vector indexes of the partition of a backing map up to date, or
     * null.
     */
    @Nullable
    MutationObserver<Record> createMapObserver(RecordStore<Record> recordStore);
}
