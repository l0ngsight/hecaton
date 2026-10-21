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

import uk.org.hecaton.cache.impl.CacheRecordObserver;
import uk.org.hecaton.cache.impl.ICacheRecordStore;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.hotrestart.HotRestartService;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.hotrestart.InternalHotRestartService;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.MutationObserver;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.version.Version;

import javax.annotation.Nullable;
import java.util.UUID;

/**
 * Persistence (F10, storage for F9, F16, F18 and F19 later), provided by
 * {@code uk.org.hecaton:hecaton-persistence}: data and cluster metadata kept on disk, so a
 * cluster can be restarted with its data. Used by the member only when persistence is enabled.
 */
public non-sealed interface PersistenceCapability extends MemberCapability {

    /**
     * Prepares persistence before the member is created: claims the member's directory and reads
     * what the member left there when it last ran. Called once, before {@link MemberModule#init}.
     *
     * @param newUuid the UUID of a member that has not run before
     * @return the UUID the member had when it last ran, or {@code newUuid}
     */
    UUID start(Node node, UUID newUuid);

    /** The member's persistence state, as core sees it. */
    InternalHotRestartService internalHotRestartService();

    /** The public persistence service (backups). */
    HotRestartService hotRestartService();

    /**
     * An observer that writes the changes of a map's record store to disk, or {@code null} if the
     * map does not persist its data.
     */
    @Nullable
    MutationObserver<Record> createMapObserver(RecordStore<Record> recordStore);

    /**
     * An observer that writes the record changes of a cache's record store to disk, or {@code null}
     * if the cache does not persist its data.
     */
    @Nullable
    CacheRecordObserver createCacheObserver(ICacheRecordStore recordStore);

    /** Called just before the member joins the cluster. */
    void beforeJoin();

    /** Called once the member has joined the cluster: starts the recovery of persisted data. */
    void afterStart();

    /** The member list changed. */
    void onMemberListChange();

    /** The partition table changed. */
    void onPartitionStateChange();

    /** The cluster state changed. */
    void onClusterStateChange(ClusterState newState);

    /** The cluster version changed. */
    void onClusterVersionChange(Version newVersion);
}
