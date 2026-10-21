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

package uk.org.hecaton.vector;

import uk.org.hecaton.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.util.Iterator;

/**
 * The results of a vector search: the most similar documents, best first.
 *
 * @param <K> key type
 * @param <V> value type
 */
@Beta
public interface SearchResults<K, V> extends Iterable<SearchResult<K, V>> {

    /** The number of results. */
    int size();

    /** The results, best first. */
    @Nonnull
    Iterator<SearchResult<K, V>> results();

    @Nonnull
    @Override
    default Iterator<SearchResult<K, V>> iterator() {
        return results();
    }
}
