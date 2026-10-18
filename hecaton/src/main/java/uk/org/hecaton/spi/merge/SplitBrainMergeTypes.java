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

package uk.org.hecaton.spi.merge;

import uk.org.hecaton.cardinality.impl.hyperloglog.HyperLogLog;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.cp.IAtomicLong;
import uk.org.hecaton.cp.IAtomicReference;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.multimap.MultiMap;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.scheduledexecutor.impl.ScheduledTaskDescriptor;
import uk.org.hecaton.vector.VectorDocument;

import java.util.Collection;

/**
 * Collection of interfaces which define the provided merge types for each data structure.
 * <p>
 * Useful for implementors of {@link SplitBrainMergePolicy} to check on which data structures
 * their custom merge policy can be configured.
 * <p>
 * Used by the {@link uk.org.hecaton.internal.config.ConfigValidator} to check if a
 * configured {@link SplitBrainMergePolicy} is usable on its data structure.
 *
 * @since 3.10
 */
public class SplitBrainMergeTypes {

    /**
     * Provided merge types of {@link IMap}.
     *
     * @param <K> key type
     * @param <V> value type
     * @since 3.10
     */
    public interface MapMergeTypes<K, V>
            extends MergingEntry<K, V>, MergingCreationTime, MergingHits, MergingLastAccessTime, MergingLastUpdateTime,
                    MergingTTL, MergingMaxIdle, MergingCosts, MergingVersion, MergingExpirationTime, MergingLastStoredTime {
    }

    /**
     * Provided merge types of {@link uk.org.hecaton.cache.ICache}.
     *
     * @param <K> key type
     * @param <V> value type
     * @since 3.10
     */
    public interface CacheMergeTypes<K, V>
            extends MergingEntry<K, V>, MergingCreationTime, MergingHits, MergingLastAccessTime, MergingExpirationTime {
    }

    /**
     * Provided merge types of {@link ReplicatedMap}.
     *
     * @param <K> key type
     * @param <V> value type
     * @since 3.10
     */
    public interface ReplicatedMapMergeTypes<K, V>
            extends MergingEntry<K, V>, MergingCreationTime, MergingHits, MergingLastAccessTime, MergingLastUpdateTime,
                    MergingTTL {
    }

    /**
     * Provided merge types of {@link MultiMap}.
     *
     * @param <K> key type
     * @param <V> value type
     * @since 3.10
     */
    public interface MultiMapMergeTypes<K, V>
            extends MergingEntry<K, Collection<V>>, MergingCreationTime, MergingHits, MergingLastAccessTime,
                    MergingLastUpdateTime {
    }

    /**
     * Provided merge types of collections ({@link ISet} and {@link IList}).
     *
     * @param <V> value type
     * @since 3.10
     */
    public interface CollectionMergeTypes<V> extends MergingValue<Collection<V>> {
    }

    /**
     * Provided merge types of {@link IQueue}.
     *
     * @param <V> value type
     * @since 3.10
     */
    public interface QueueMergeTypes<V> extends MergingValue<Collection<V>> {
    }

    /**
     * Provided merge types of {@link uk.org.hecaton.ringbuffer.Ringbuffer}.
     *
     * @since 3.10
     */
    public interface RingbufferMergeTypes extends MergingValue<RingbufferMergeData> {
    }

    /**
     * Provided merge types of {@link IAtomicLong}.
     *
     * @since 3.10
     */
    public interface AtomicLongMergeTypes extends MergingValue<Long> {
    }

    /**
     * Provided merge types of {@link IAtomicReference}.
     *
     * @since 3.10
     */
    public interface AtomicReferenceMergeTypes extends MergingValue<Object> {
    }

    /**
     * Provided merge types of {@link uk.org.hecaton.scheduledexecutor.IScheduledExecutorService}.
     *
     * @since 3.10
     */
    public interface ScheduledExecutorMergeTypes extends MergingEntry<String, ScheduledTaskDescriptor> {
    }

    /**
     * Provided merge types of {@link uk.org.hecaton.cardinality.CardinalityEstimator}.
     *
     * @since 3.10
     */
    public interface CardinalityEstimatorMergeTypes extends MergingEntry<String, HyperLogLog> {
    }

    /**
     * Merge type of {@code VectorCollection}.
     * @param <K>   type of {@code VectorCollection} key
     * @param <V>   type of {@link VectorDocument}, stored in the {@code VectorCollection}
     * @since 5.6
     */
    public interface VectorCollectionMergeTypes<K, V extends VectorDocument<?>> extends MergingEntry<K, V> {
    }
}
