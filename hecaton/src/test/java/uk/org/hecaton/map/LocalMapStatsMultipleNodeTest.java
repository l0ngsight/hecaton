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

package uk.org.hecaton.map;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.config.MemberGroupConfig;
import uk.org.hecaton.config.PartitionGroupConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.map.listener.EntryEvictedListener;
import uk.org.hecaton.multimap.MultiMap;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalMapStatsMultipleNodeTest extends HecatonTestSupport {

    //https://github.com/hazelcast/hazelcast/issues/21785
    @Test
    public void testHits_isZero_afterPutAndReplicated() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance instance1 = factory.newHecatonInstance(getConfig());
        IMap<Integer, Integer> iMap = instance1.getMap("test-hits");

        for (int i = 0; i < 100; i++) {
            iMap.put(i, i);
        }

        HecatonInstance instance2 = factory.newHecatonInstance(getConfig());
        HecatonInstance instance3 = factory.newHecatonInstance(getConfig());
        waitAllForSafeState(instance1, instance2, instance3);
        assertEquals(0, instance1.getMap("test-hits").getLocalMapStats().getHits());
        assertEquals(0, instance2.getMap("test-hits").getLocalMapStats().getHits());
        assertEquals(0, instance3.getMap("test-hits").getLocalMapStats().getHits());
    }

    @Test
    public void testHits_whenMultipleNodes() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance[] instances = factory.newInstances(getConfig());
        MultiMap<Object, Object> multiMap0 = instances[0].getMultiMap("testHits_whenMultipleNodes");
        MultiMap<Object, Object> multiMap1 = instances[1].getMultiMap("testHits_whenMultipleNodes");

        // InternalPartitionService is used in order to determine owners of these keys that we will use.
        InternalPartitionService partitionService = getNode(instances[0]).getPartitionService();
        Address address = partitionService.getPartitionOwner(partitionService.getPartitionId("test1"));

        boolean inFirstInstance = address.equals(getNode(instances[0]).getThisAddress());
        multiMap0.get("test0");

        multiMap0.put("test1", 1);
        multiMap1.get("test1");

        assertEquals(inFirstInstance ? 1 : 0, multiMap0.getLocalMultiMapStats().getHits());
        assertEquals(inFirstInstance ? 0 : 1, multiMap1.getLocalMultiMapStats().getHits());

        multiMap0.get("test1");
        multiMap1.get("test1");
        assertEquals(inFirstInstance ? 0 : 3, multiMap1.getLocalMultiMapStats().getHits());
    }

    @Test
    public void testPutStats_afterPutAll() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance[] instances = factory.newInstances(getConfig());
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 1; i <= 5000; i++) {
            map.put(i, i);
        }

        IMap<Integer, Integer> iMap = instances[0].getMap("example");
        iMap.putAll(map);
        final LocalMapStats localMapStats = iMap.getLocalMapStats();
        assertTrueEventually(() -> assertEquals(5000, localMapStats.getPutOperationCount()));
    }

    @Test
    public void testLocalMapStats_withMemberGroups() {
        final String mapName = randomMapName();
        final String[] firstMemberGroup = {"127.0.0.1", "127.0.0.2"};
        final String[] secondMemberGroup = {"127.0.0.3"};

        final Config config = createConfig(mapName, firstMemberGroup, secondMemberGroup);
        final String[] addressArray = concatenateArrays(firstMemberGroup, secondMemberGroup);

        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(addressArray);
        final HecatonInstance node1 = factory.newHecatonInstance(config);
        final HecatonInstance node2 = factory.newHecatonInstance(config);
        final HecatonInstance node3 = factory.newHecatonInstance(config);

        final IMap<Object, Object> test = node3.getMap(mapName);
        test.put(1, 1);

        assertBackupEntryCount(1, mapName, factory.getAllHecatonInstances());
    }

    @Test
    public void testLocalMapStats_preservedAfterEviction() {
        String mapName = randomMapName();
        Config config = new Config();
        config.getProperties().setProperty(ClusterProperty.PARTITION_COUNT.getName(), "5");
        MapConfig mapConfig = config.getMapConfig(mapName);

        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);
        evictionConfig.setMaxSizePolicy(MaxSizePolicy.PER_PARTITION);
        evictionConfig.setSize(25);

        HecatonInstance instance = createHecatonInstance(config);
        IMap<Object, Object> map = instance.getMap(mapName);
        final CountDownLatch entryEvictedLatch = new CountDownLatch(700);
        map.addEntryListener((EntryEvictedListener<Object, Object>) event -> entryEvictedLatch.countDown(), true);
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
            map.set(i, i);
            assertEquals(i, map.get(i));
        }

        // eviction happens after response return, this
        // is why we have eventual assertion here.
        assertTrueEventually(() -> {
            LocalMapStats localMapStats = map.getLocalMapStats();
            assertEquals(2000, localMapStats.getHits());
            assertEquals(1000, localMapStats.getPutOperationCount());
            assertEquals(1000, localMapStats.getSetOperationCount());
            assertEquals(1000, localMapStats.getGetOperationCount());
        });

        assertOpenEventually(entryEvictedLatch);

        assertTrueEventually(() -> {
            LocalMapStats localMapStats = map.getLocalMapStats();
            assertEquals(2000, localMapStats.getHits());
            assertEquals(1000, localMapStats.getPutOperationCount());
            assertEquals(1000, localMapStats.getSetOperationCount());
            assertEquals(1000, localMapStats.getGetOperationCount());
        });
    }

    private void assertBackupEntryCount(final long expectedBackupEntryCount, final String mapName,
                                        final Collection<HecatonInstance> nodes) {
        assertTrueEventually(() -> {
            long backup = 0;
            for (HecatonInstance node : nodes) {
                final IMap<Object, Object> map = node.getMap(mapName);
                backup += getBackupEntryCount(map);
            }
            assertEquals(expectedBackupEntryCount, backup);
        });
    }

    private long getBackupEntryCount(IMap<Object, Object> map) {
        final LocalMapStats localMapStats = map.getLocalMapStats();
        return localMapStats.getBackupEntryCount();
    }

    private String[] concatenateArrays(String[]... arrays) {
        int len = 0;
        for (String[] array : arrays) {
            len += array.length;
        }
        String[] result = new String[len];
        int destPos = 0;
        for (String[] array : arrays) {
            System.arraycopy(array, 0, result, destPos, array.length);
            destPos += array.length;
        }
        return result;
    }

    private Config createConfig(String mapName, String[] firstGroup, String[] secondGroup) {
        final MemberGroupConfig firstGroupConfig = createGroupConfig(firstGroup);
        final MemberGroupConfig secondGroupConfig = createGroupConfig(secondGroup);

        Config config = getConfig();
        config.getPartitionGroupConfig().setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.CUSTOM);
        config.getPartitionGroupConfig().addMemberGroupConfig(firstGroupConfig);
        config.getPartitionGroupConfig().addMemberGroupConfig(secondGroupConfig);
        config.getNetworkConfig().getInterfaces().addInterface("127.0.0.*");

        config.getMapConfig(mapName).setBackupCount(2);

        return config;
    }

    private MemberGroupConfig createGroupConfig(String[] addressArray) {
        final MemberGroupConfig memberGroupConfig = new MemberGroupConfig();
        for (String address : addressArray) {
            memberGroupConfig.addInterface(address);
        }
        return memberGroupConfig;
    }

}
