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

package uk.org.hecaton.wan.impl.batch;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ConsistencyCheckStrategy;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MerkleTreeConfig;
import uk.org.hecaton.config.WanBatchPublisherConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.config.WanSyncConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.monitor.LocalWanPublisherStats;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.wan.impl.ConsistencyCheckResult;
import uk.org.hecaton.wan.impl.WanReplicationService;
import uk.org.hecaton.wan.impl.WanSyncStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * WAN sync and consistency check (F13): entries the target cluster missed while the publisher was
 * stopped reach it by a full or a Merkle tree sync; the consistency check finds the difference.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class WanSyncTest extends HecatonTestSupport {

    private static final String WAN = "to-b";
    private static final String MAP = "synced";
    private static final int ENTRIES = 1000;

    private TestHecatonInstanceFactory factory;

    @Before
    public void setUp() {
        factory = createHecatonInstanceFactory(4);
    }

    @Test
    public void fullSyncSendsAllEntries() {
        List<HecatonInstance> target = startCluster("b", 2, null, false);
        List<HecatonInstance> source = startCluster("a", 2, publisher(target, ConsistencyCheckStrategy.NONE), false);
        fillWhileStopped(source, target);

        UUID syncId = wanService(source.get(0)).syncMap(WAN, "b", MAP);
        assertThat(syncId).isNotNull();
        assertTrueEventually(() -> assertThat(target.get(0).getMap(MAP).size()).isEqualTo(ENTRIES));
        assertTrueEventually(() -> {
            int synced = 0;
            for (HecatonInstance member : source) {
                assertThat(wanService(member).getWanSyncState().getStatus()).isEqualTo(WanSyncStatus.READY);
                synced += publisherStats(member).getLastSyncStats().get(MAP).getRecordsSynced();
            }
            assertThat(synced).isEqualTo(ENTRIES);
        });
    }

    @Test
    public void merkleTreeSyncSendsTheEntriesThatDiffer() {
        List<HecatonInstance> target = startCluster("b", 2, null, true);
        List<HecatonInstance> source = startCluster("a", 2, publisher(target, ConsistencyCheckStrategy.MERKLE_TREES), true);
        // the target has half of the entries, one of them with another value
        IMap<Integer, String> targetMap = target.get(0).getMap(MAP);
        for (int i = 0; i < ENTRIES / 2; i++) {
            targetMap.put(i, "value" + i);
        }
        targetMap.put(0, "other");
        fillWhileStopped(source, target);

        assertThat(consistencyCheck(source).getLastDiffLeafCount()).isPositive();

        wanService(source.get(0)).syncMap(WAN, "b", MAP);
        assertTrueEventually(() -> {
            assertThat(targetMap.size()).isEqualTo(ENTRIES);
            assertThat(targetMap.get(0)).isEqualTo("value0");
        });
        int synced = 0;
        for (HecatonInstance member : source) {
            assertTrueEventually(() -> assertThat(publisherStats(member).getLastSyncStats()).containsKey(MAP));
            synced += publisherStats(member).getLastSyncStats().get(MAP).getRecordsSynced();
        }
        // only the entries of the leaves that differ were sent
        assertThat(synced).isLessThan(ENTRIES).isGreaterThanOrEqualTo(ENTRIES / 2);

        assertThat(consistencyCheck(source).getLastDiffLeafCount()).isZero();
    }

    private ConsistencyCheckResult consistencyCheck(List<HecatonInstance> source) {
        UUID checkId = wanService(source.get(0)).consistencyCheck(WAN, "b", MAP);
        assertThat(checkId).isNotNull();
        ConsistencyCheckResult[] total = new ConsistencyCheckResult[1];
        assertTrueEventually(() -> {
            int diffLeaves = 0;
            int checkedPartitions = 0;
            for (HecatonInstance member : source) {
                ConsistencyCheckResult result = publisherStats(member).getLastConsistencyCheckResults().get(MAP);
                assertThat(result).isNotNull();
                assertThat(result.getUuid()).isEqualTo(checkId);
                diffLeaves += result.getLastDiffLeafCount();
                checkedPartitions += result.getLastCheckedPartitionCount();
            }
            assertThat(checkedPartitions).isEqualTo(31);
            total[0] = new ConsistencyCheckResult(checkId, checkedPartitions, 0, 0, diffLeaves, 0);
        });
        return total[0];
    }

    private void fillWhileStopped(List<HecatonInstance> source, List<HecatonInstance> target) {
        source.forEach(member -> wanService(member).stop(WAN, "b"));
        IMap<Integer, String> map = source.get(0).getMap(MAP);
        for (int i = 0; i < ENTRIES; i++) {
            map.put(i, "value" + i);
        }
        source.forEach(member -> wanService(member).resume(WAN, "b"));
        sleepSeconds(1);
        assertThat(target.get(0).getMap(MAP).size()).isLessThan(ENTRIES);
    }

    private WanBatchPublisherConfig publisher(List<HecatonInstance> target, ConsistencyCheckStrategy strategy) {
        String endpoints = target.stream()
                .map(member -> member.getCluster().getLocalMember().getAddress().getHost() + ":"
                        + member.getCluster().getLocalMember().getAddress().getPort())
                .collect(Collectors.joining(","));
        WanBatchPublisherConfig publisher = new WanBatchPublisherConfig().setClusterName("b").setTargetEndpoints(endpoints)
                .setBatchSize(100).setBatchMaxDelayMillis(50);
        publisher.setSyncConfig(new WanSyncConfig().setConsistencyCheckStrategy(strategy));
        return publisher;
    }

    private List<HecatonInstance> startCluster(String name, int size, WanBatchPublisherConfig publisher, boolean merkle) {
        Config config = smallInstanceConfig().setClusterName(name);
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "31");
        MapConfig mapConfig = new MapConfig(MAP);
        if (merkle) {
            mapConfig.setMerkleTreeConfig(new MerkleTreeConfig().setEnabled(true).setDepth(6));
        }
        if (publisher != null) {
            config.addWanReplicationConfig(new WanReplicationConfig().setName(WAN).addBatchReplicationPublisherConfig(publisher));
            mapConfig.setWanReplicationRef(new WanReplicationRef().setName(WAN)
                    .setMergePolicyClassName(PassThroughMergePolicy.class.getName()));
        }
        config.addMapConfig(mapConfig);
        List<HecatonInstance> members = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            members.add(factory.newHecatonInstance(config));
        }
        assertClusterSizeEventually(size, members);
        return members;
    }

    private static LocalWanPublisherStats publisherStats(HecatonInstance member) {
        return wanService(member).getStats().get(WAN).getLocalWanPublisherStats().get("b");
    }

    private static WanReplicationService wanService(HecatonInstance member) {
        return getNode(member).getNodeEngine().getWanReplicationService();
    }
}
