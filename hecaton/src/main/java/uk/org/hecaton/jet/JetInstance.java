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

package uk.org.hecaton.jet;

import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.spi.annotation.Beta;
import uk.org.hecaton.sql.SqlService;
import uk.org.hecaton.topic.ITopic;

import javax.annotation.Nonnull;

/**
 * @since Jet 3.0
 *
 * @deprecated After 5.0 Jet was merged into core Hecaton product. Jet
 * became a service of Hecaton instead of being an instance of its own
 * that encapsulates {@link HecatonInstance}. Please use {@link
 * JetService} instead.
 */
@Deprecated(since = "5.0", forRemoval = true)
public interface JetInstance extends JetService {

    /**
     * Returns the underlying Hecaton instance used by Jet. It will
     * be either a server node or a client, depending on the type of this
     * {@code JetInstance}.
     *
     * @since Jet 3.0
     *
     * @deprecated
     * Because we first access to {@link HecatonInstance} and then
     * {@link JetService} from the product's entry point -{@link Hecaton}-,
     * we don't need to this back reference anymore. This class made
     * sense when the entry point was {@link Jet}.
     */
    @Nonnull
    @Deprecated(since = "5.0", forRemoval = true)
    HecatonInstance getHecatonInstance();

    /**
     * @since Jet 3.0
     * @deprecated
     * Use {@link HecatonInstance#getName()} instead.
     */
    @Nonnull
    @Deprecated(since = "5.0", forRemoval = true)
    String getName();

    /**
     * @since Jet 3.0
     * @deprecated
     * Use {@link HecatonInstance#getCluster()} instead.
     */
    @Nonnull
    @Deprecated(since = "5.0", forRemoval = true)
    Cluster getCluster();

    /**
     * @since Jet 4.4
     * @deprecated
     * Use {@link HecatonInstance#getSql()} instead.
     */
    @Beta
    @Nonnull
    @Deprecated(since = "5.0", forRemoval = true)
    SqlService getSql();

    /**
     * @since Jet 3.0
     * @deprecated
     * Use {@link HecatonInstance#getMap(String)} instead.
     */
    @Nonnull
    @Deprecated(since = "5.0", forRemoval = true)
    <K, V> IMap<K, V> getMap(@Nonnull String name);

    /**
     * @since Jet 3.0
     * @deprecated
     * Use {@link HecatonInstance#getReplicatedMap(String)} instead.
     */
    @Nonnull
    @Deprecated(since = "5.0", forRemoval = true)
    <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name);

    /**
     * @since Jet 3.0
     * @deprecated
     * Use {@link HecatonInstance#getList(String)} instead.
     */
    @Nonnull
    @Deprecated(since = "5.0", forRemoval = true)
    <E> IList<E> getList(@Nonnull String name);

    /**
     * @since Jet 3.0
     * @deprecated
     * Use {@link HecatonInstance#getReliableTopic(String)} instead.
     */
    @Nonnull
    @Deprecated(since = "5.0", forRemoval = true)
    <E> ITopic<E> getReliableTopic(@Nonnull String name);

    /**
     * @since Jet 3.0
     * @deprecated
     * Use {@link HecatonInstance#getCacheManager()} instead.
     */
    @Deprecated(since = "5.0", forRemoval = true)
    @Nonnull
    JetCacheManager getCacheManager();

    /**
     * @deprecated
     * Use {@link HecatonInstance#shutdown()} instead.
     */
    @Deprecated(since = "5.0", forRemoval = true)
    void shutdown();
}
