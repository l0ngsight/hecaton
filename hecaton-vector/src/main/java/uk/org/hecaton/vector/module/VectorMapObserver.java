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

package uk.org.hecaton.vector.module;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.MutationObserver;
import uk.org.hecaton.vector.impl.DataVectorDocument;

import javax.annotation.Nonnull;

/**
 * Keeps the indexes of a partition of a vector collection in step with its backing map's record
 * store: every write to a replica, by an operation, a backup or a migration, is indexed.
 */
final class VectorMapObserver implements MutationObserver<Record> {

    private final PartitionVectors vectors;
    private final SerializationService serializationService;
    private final Runnable onDestroy;

    VectorMapObserver(PartitionVectors vectors, SerializationService serializationService, Runnable onDestroy) {
        this.vectors = vectors;
        this.serializationService = serializationService;
        this.onDestroy = onDestroy;
    }

    @Override
    public void onPutRecord(@Nonnull Data key, Record record, Object oldValue, boolean backup) {
        index(key, record.getValue());
    }

    @Override
    public void onReplicationPutRecord(@Nonnull Data key, @Nonnull Record record, boolean populateIndex) {
        index(key, record.getValue());
    }

    @Override
    public void onUpdateRecord(@Nonnull Data key, @Nonnull Record record, Object oldValue, Object newValue,
                               boolean backup) {
        index(key, newValue != null ? newValue : record.getValue());
    }

    @Override
    public void onRemoveRecord(@Nonnull Data key, Record record, boolean backup) {
        vectors.remove(key);
    }

    @Override
    public void onEvictRecord(@Nonnull Data key, @Nonnull Record record, boolean backup) {
        vectors.remove(key);
    }

    @Override
    public void onLoadRecord(@Nonnull Data key, @Nonnull Record record, boolean backup) {
        index(key, record.getValue());
    }

    @Override
    public void onReset() {
        vectors.clear();
    }

    @Override
    public void onClear() {
        vectors.clear();
    }

    @Override
    public void onDestroy(boolean isDuringShutdown, boolean internal) {
        vectors.clear();
        onDestroy.run();
    }

    private void index(Data key, Object value) {
        Object document = value instanceof Data data ? serializationService.toObject(data) : value;
        if (document instanceof DataVectorDocument vectorDocument) {
            vectors.put(key, vectorDocument);
        } else {
            vectors.remove(key);
        }
    }
}
