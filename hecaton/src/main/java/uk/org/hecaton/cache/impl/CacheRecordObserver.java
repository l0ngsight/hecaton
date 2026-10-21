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
package uk.org.hecaton.cache.impl;

import uk.org.hecaton.cache.impl.record.CacheRecord;
import uk.org.hecaton.internal.serialization.Data;

/**
 * Observes the changes of the records of a cache's record store, for example to persist them.
 * Called on the partition's thread, after the change.
 */
public interface CacheRecordObserver {

    /** Observes nothing. */
    CacheRecordObserver NONE = new CacheRecordObserver() {
        @Override
        public void onPut(Data key, CacheRecord record) {
        }

        @Override
        public void onRemove(Data key) {
        }

        @Override
        public void onClear() {
        }
    };

    /** A record was added, or its value, expiration time or expiry policy changed. */
    void onPut(Data key, CacheRecord record);

    /** The record of the key was removed (deleted, evicted or expired). */
    void onRemove(Data key);

    /** All records were removed; not called when the member shuts down. */
    void onClear();
}
