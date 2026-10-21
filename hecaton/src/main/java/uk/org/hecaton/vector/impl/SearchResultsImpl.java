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

package uk.org.hecaton.vector.impl;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.vector.SearchResult;
import uk.org.hecaton.vector.SearchResults;
import uk.org.hecaton.vector.VectorDocument;
import uk.org.hecaton.vector.VectorValues;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Search results deserialized for the caller, and conversions between the serialized and
 * deserialized forms of documents.
 */
public final class SearchResultsImpl<K, V> implements SearchResults<K, V> {

    private final List<SearchResult<K, V>> results;

    public SearchResultsImpl(List<SearchResult<K, V>> results) {
        this.results = Collections.unmodifiableList(results);
    }

    /** Deserializes results received from members (best first). */
    public static <K, V> SearchResults<K, V> of(List<DataSearchResult> results, SerializationService serializationService) {
        List<SearchResult<K, V>> deserialized = new ArrayList<>(results.size());
        for (DataSearchResult result : results) {
            deserialized.add(new Result<>(serializationService.toObject(result.getKey()),
                    serializationService.toObject(result.getValue()), result.getScore(), result.getVectors()));
        }
        return new SearchResultsImpl<>(deserialized);
    }

    /** A document with its value serialized. */
    public static DataVectorDocument toData(VectorDocument<?> document, SerializationService serializationService) {
        return new DataVectorDocument(serializationService.toData(document.getValue()), document.getVectors());
    }

    /** A document with its value deserialized, or null. */
    @Nullable
    public static <V> VectorDocument<V> toObject(@Nullable DataVectorDocument document,
                                                 SerializationService serializationService) {
        if (document == null) {
            return null;
        }
        Data value = document.getValue();
        return new VectorDocumentImpl<>(serializationService.toObject(value), document.getVectors());
    }

    @Override
    public int size() {
        return results.size();
    }

    @Nonnull
    @Override
    public Iterator<SearchResult<K, V>> results() {
        return results.iterator();
    }

    @Override
    public String toString() {
        return "SearchResults" + results;
    }

    private record Result<K, V>(K key, V value, float score, VectorValues vectors) implements SearchResult<K, V> {

        @Nonnull
        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public VectorValues getVectors() {
            return vectors;
        }

        @Override
        public float getScore() {
            return score;
        }
    }
}
