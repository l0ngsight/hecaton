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

package uk.org.hecaton.client.map;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.monitor.impl.LocalMapStatsImpl;
import uk.org.hecaton.internal.monitor.impl.LocalIndexStatsImpl;
import uk.org.hecaton.internal.monitor.impl.PerIndexStats;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.LocalIndexStatsTest;
import uk.org.hecaton.map.LocalMapStats;
import uk.org.hecaton.query.LocalIndexStats;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.query.impl.IndexRegistry;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static uk.org.hecaton.test.Accessors.getAllIndexes;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientIndexStatsTest extends LocalIndexStatsTest {

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    private Config finalConfig;
    private HecatonInstance member1;
    private HecatonInstance member2;

    protected IMap map1;
    protected IMap map2;

    protected IMap noStatsMap1;
    protected IMap noStatsMap2;

    @Override
    protected HecatonInstance createInstance(Config config) {
        finalConfig = config;
        member1 = hecatonFactory.newHecatonInstance(config);
        member2 = hecatonFactory.newHecatonInstance(config);

        map1 = member1.getMap(mapName);
        map2 = member2.getMap(mapName);

        noStatsMap1 = member1.getMap(noStatsMapName);
        noStatsMap2 = member2.getMap(noStatsMapName);

        return hecatonFactory.newHecatonClient(new ClientConfig());
    }

    @After
    public void after() {
        hecatonFactory.terminateAll();
    }

    @Override
    protected void ensureSafeState() {
        waitAllForSafeState(hecatonFactory.getAllHecatonInstances());
    }

    @Test
    @Override
    public void testQueryCounting_WhenPartitionPredicateIsUsed() {
        addIndex(map, "this", false);

        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }

        map.entrySet(Predicates.partitionPredicate(10, Predicates.equal("this", 10)));
        assertTrue(map1.getLocalMapStats().getQueryCount() == 1 && map2.getLocalMapStats().getQueryCount() == 0
                || map1.getLocalMapStats().getQueryCount() == 0 && map2.getLocalMapStats().getQueryCount() == 1);
        assertEquals(0, map1.getLocalMapStats().getIndexedQueryCount());
        assertEquals(0, map2.getLocalMapStats().getIndexedQueryCount());
        assertEquals(0, map1.getLocalMapStats().getIndexStats().get("this").getQueryCount());
        assertEquals(0, map2.getLocalMapStats().getIndexStats().get("this").getQueryCount());
    }

    @Test
    @Ignore
    @Override
    public void testAverageQuerySelectivityCalculation_WhenSomePartitionsAreEmpty() {
        // do nothing
    }

    @Test
    public void shouldUseIndexFromClient_whenMemberProxyExists() {
        testIndexWithoutMapProxy((s) -> {});
    }

    @Test
    @Ignore("HZ-4455")
    public void shouldUseIndexFromClient_whenMemberProxyDestroyed() {
        testIndexWithoutMapProxy((mapName) -> member1.getMap(mapName).destroy());
    }

    @Test
    public void shouldUseIndexFromClient_whenClusterRestarted() {
        warmUpPartitions(member1, member2);
        testIndexWithoutMapProxy((mapName) -> restartCluster(true, ClusterState.ACTIVE, mapName));
    }

    @Test
    public void shouldUseIndexFromClient_whenClusterRestartedForcefully() {
        warmUpPartitions(member1, member2);
        testIndexWithoutMapProxy((mapName) -> restartCluster(false, ClusterState.ACTIVE, mapName));
    }

    @Test
    public void shouldUseIndexFromClient_whenClusterRestartedInPassiveState() {
        warmUpPartitions(member1, member2);
        testIndexWithoutMapProxy((mapName) -> restartCluster(true, ClusterState.PASSIVE, mapName));
    }

    @Test
    public void shouldUseIndexFromClient_whenClusterRestartedInFrozenState() {
        warmUpPartitions(member1, member2);
        testIndexWithoutMapProxy((mapName) -> restartCluster(true, ClusterState.FROZEN, mapName));
    }

    private void testIndexWithoutMapProxy(Consumer<String> actionBeforeTest) {
        // given map with static index
        String indexMapName = randomMapName() + "_client";
        // inherit default config
        MapConfig mapWithIndex = new MapConfig(finalConfig.getMapConfig(mapName))
                .setName(indexMapName)
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "this").setName("index"));
        member1.getConfig().addMapConfig(mapWithIndex);

        var clientMap = instance.getMap(indexMapName);

        // when
        actionBeforeTest.accept(indexMapName);

        for (int i = 0; i < 100; ++i) {
            clientMap.put(i, i);
        }
        for (int i = 0; i < 100; ++i) {
            assertThat(clientMap.entrySet(Predicates.equal("this", i)))
                    .hasSize(1)
                    .containsOnly(Map.entry(i, i));
        }

        // then
        // get member maps after test not to create proxies accidentally earlier
        map1 = member1.getMap(indexMapName);
        map2 = member2.getMap(indexMapName);

        assertThat(stats().getQueryCount()).isEqualTo(100);
        assertThat(stats().getIndexedQueryCount()).isEqualTo(100);
    }

    private void assertMapProxyInitializedEventually(String indexMapName) {
        assertTrueEventually(() -> {
            // In some circumstances getDistributedObjects may not return all objects,
            // but that should not happen in this test.
            assertThat(member1.getDistributedObjects().stream().map(DistributedObject::getName))
                    .as("Should initialize proxy on member1")
                    .contains(indexMapName);
            assertThat(member2.getDistributedObjects().stream().map(DistributedObject::getName))
                    .as("Should initialize proxy on member2")
                    .contains(indexMapName);
        });
    }

    private void restartCluster(boolean graceful, ClusterState restartInState, String indexMapName) {
        // Avoid race conditions between async proxy initialization on other members (via events)
        // and cluster restart. Those races are unfortunate but are not tested here.
        assertMapProxyInitializedEventually(indexMapName);

        warmUpPartitions(member1, member2);
        member1.getCluster().changeClusterState(restartInState);
        member1 = restartMember(member1, member2, graceful);

        // do not restart the other member until proxy is fully initialized on first member
        // so the proxy can be included in PostJoinProxyOperation. Proxy initialization may take
        // some time (especially when backup operations fail) and is async process.
        assertMapProxyInitializedEventually(indexMapName);

        member2 = restartMember(member2, member1, graceful);

        member1.getCluster().changeClusterState(ClusterState.ACTIVE);

        // Wait for proxy initialization by post join operation to avoid race condition.
        // Note that indexes are initialized in many ways, proxy initialization being only one
        // of them. So even without proxy indexes might work but with proxy they will for sure.
        assertMapProxyInitializedEventually(indexMapName);
    }

    private HecatonInstance restartMember(HecatonInstance member, HecatonInstance otherMember, boolean graceful) {
        Address address1 = Accessors.getAddress(member);
        if (graceful) {
            member.shutdown();
        } else {
            member.getLifecycleService().terminate();
        }
        assertClusterSizeEventually(1, otherMember);
        waitAllForSafeState(otherMember);
        HecatonInstance restartedMember = hecatonFactory.newHecatonInstance(address1, finalConfig);
        assertClusterSizeEventually(2, restartedMember, otherMember);
        waitAllForSafeState(restartedMember, otherMember);
        return restartedMember;
    }

    @Override
    protected LocalMapStats stats() {
        return combineStats(map1, map2);
    }

    @Override
    protected LocalMapStats noStats() {
        return combineStats(noStatsMap1, noStatsMap2);
    }

    private static LocalMapStats combineStats(IMap map1, IMap map2) {
        LocalMapStats stats1 = map1.getLocalMapStats();
        LocalMapStats stats2 = map2.getLocalMapStats();

        List<IndexRegistry> allIndexes = new ArrayList<>();
        allIndexes.addAll(getAllIndexes(map1));
        allIndexes.addAll(getAllIndexes(map2));

        LocalMapStatsImpl combinedStats = new LocalMapStatsImpl();

        assertEquals(stats1.getQueryCount(), stats2.getQueryCount());
        combinedStats.setQueryCount(stats1.getQueryCount());
        assertEquals(stats1.getIndexedQueryCount(), stats2.getIndexedQueryCount());
        combinedStats.setIndexedQueryCount(stats1.getIndexedQueryCount());

        assertEquals(stats1.getIndexStats().size(), stats2.getIndexStats().size());
        Map<String, LocalIndexStatsImpl> combinedIndexStatsMap = new HashMap<>();
        for (Map.Entry<String, LocalIndexStats> indexEntry : stats1.getIndexStats().entrySet()) {
            LocalIndexStats indexStats1 = indexEntry.getValue();
            LocalIndexStats indexStats2 = stats2.getIndexStats().get(indexEntry.getKey());
            assertNotNull(indexStats2);

            LocalIndexStatsImpl combinedIndexStats = new LocalIndexStatsImpl();
            assertEquals(indexStats1.getHitCount(), indexStats2.getHitCount());
            combinedIndexStats.setHitCount(indexStats1.getHitCount());

            assertEquals(indexStats1.getQueryCount(), indexStats2.getQueryCount());
            combinedIndexStats.setQueryCount(indexStats1.getQueryCount());
            combinedIndexStats
                    .setAverageHitLatency((indexStats1.getAverageHitLatency() + indexStats2.getAverageHitLatency()) / 2);

            long totalHitCount = 0;
            double totalNormalizedHitCardinality = 0.0;
            for (IndexRegistry indexes : allIndexes) {
                PerIndexStats perIndexStats = indexes.getIndex(indexEntry.getKey()).getPerIndexStats();
                totalHitCount += perIndexStats.getHitCount();
                totalNormalizedHitCardinality += perIndexStats.getTotalNormalizedHitCardinality();
            }
            combinedIndexStats
                    .setAverageHitSelectivity(totalHitCount == 0 ? 0.0 : 1.0 - totalNormalizedHitCardinality / totalHitCount);

            combinedIndexStats.setInsertCount(indexStats1.getInsertCount() + indexStats2.getInsertCount());
            combinedIndexStats.setTotalInsertLatency(indexStats1.getTotalInsertLatency() + indexStats2.getTotalInsertLatency());

            combinedIndexStats.setUpdateCount(indexStats1.getUpdateCount() + indexStats2.getUpdateCount());
            combinedIndexStats.setTotalUpdateLatency(indexStats1.getTotalUpdateLatency() + indexStats2.getTotalUpdateLatency());

            combinedIndexStats.setRemoveCount(indexStats1.getRemoveCount() + indexStats2.getRemoveCount());
            combinedIndexStats.setTotalRemoveLatency(indexStats1.getTotalRemoveLatency() + indexStats2.getTotalRemoveLatency());

            combinedIndexStats.setMemoryCost(indexStats1.getMemoryCost() + indexStats2.getMemoryCost());

            combinedIndexStatsMap.put(indexEntry.getKey(), combinedIndexStats);
        }
        combinedStats.setIndexStats(combinedIndexStatsMap);

        return combinedStats;
    }

}
