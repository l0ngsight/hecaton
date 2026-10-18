/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.map.impl;

import uk.org.hecaton.internal.journal.EventJournalReader;
import uk.org.hecaton.map.EventJournalMapEvent;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Additional IMap methods, available and internally, but not currently exposed in public API.
 * These methods may be promoted to public API in the future.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface IMapInternal<K, V> extends IMap<K, V>, EventJournalReader<EventJournalMapEvent<K, V>> {

    CompletableFuture<Void> flushAsync();

    CompletableFuture<Void> flushAsync(@Nullable Collection<Integer> partitionsToFlush);

    InternalCompletableFuture<V> putIfAbsentAsync(@Nonnull K key, @Nonnull V value);

    InternalCompletableFuture<V> putIfAbsentAsync(@Nonnull K key, @Nonnull V value,
                                                  long ttl, @Nonnull TimeUnit timeunit);

    InternalCompletableFuture<V> putIfAbsentAsync(@Nonnull K key, @Nonnull V value,
                                                  long ttl, @Nonnull TimeUnit timeunit,
                                                  long maxIdle, @Nonnull TimeUnit maxIdleUnit);
}
