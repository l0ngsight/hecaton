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
package uk.org.hecaton.persistence.internal;

import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DataPersistenceConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.persistence.BackupTaskState;
import uk.org.hecaton.persistence.PersistenceService;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class PersistenceRestartTest extends HecatonTestSupport {

    private static final int ENTRIES = 2000;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void singleMemberRestartsWithItsData() throws IOException {
        File baseDir = folder.newFolder();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);

        HecatonInstance member = factory.newHecatonInstance(config(baseDir));
        UUID uuid = member.getCluster().getLocalMember().getUuid();
        IMap<Integer, String> map = member.getMap("persistent");
        IMap<Integer, String> notPersistent = member.getMap("transient");
        for (int i = 0; i < ENTRIES; i++) {
            map.put(i, "value" + i);
            notPersistent.put(i, "value" + i);
        }
        for (int i = 0; i < ENTRIES; i += 10) {
            map.put(i, "updated" + i);
        }
        for (int i = 1; i < ENTRIES; i += 10) {
            map.remove(i);
        }
        map.put(-1, "with ttl", 1, TimeUnit.HOURS);
        member.getCluster().shutdown();

        member = factory.newHecatonInstance(config(baseDir));
        waitUntilRecovered(member);
        assertThat(member.getCluster().getLocalMember().getUuid()).isEqualTo(uuid);
        map = member.getMap("persistent");
        assertThat(map.size()).isEqualTo(ENTRIES - ENTRIES / 10 + 1);
        assertThat(map.get(0)).isEqualTo("updated0");
        assertThat(map.get(1)).isNull();
        assertThat(map.get(2)).isEqualTo("value2");
        assertThat(map.getEntryView(-1).getTtl()).isEqualTo(TimeUnit.HOURS.toMillis(1));
        assertThat(member.getMap("transient").size()).isZero();
        assertThat(member.getCluster().getClusterState()).isEqualTo(ClusterState.ACTIVE);

        // and it keeps persisting
        map.put(1, "again");
        member.getCluster().shutdown();
        member = factory.newHecatonInstance(config(baseDir));
        waitUntilRecovered(member);
        assertThat(member.<Integer, String>getMap("persistent").get(1)).isEqualTo("again");
    }

    @Test
    public void clusterRestartsWithItsDataAndPartitionTable() throws IOException {
        File baseDir = folder.newFolder();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(6);
        List<HecatonInstance> members = startMembers(factory, baseDir, 3);
        waitAllForSafeState(members);
        IMap<Integer, String> map = members.get(0).getMap("persistent");
        for (int i = 0; i < ENTRIES; i++) {
            map.put(i, "value" + i);
        }
        Set<UUID> uuids = uuids(members);
        UUID ownerOfKey = members.get(0).getPartitionService().getPartition(42).getOwner().getUuid();
        members.get(0).getCluster().shutdown();

        // the members restart with other addresses
        List<HecatonInstance> restarted = startMembers(factory, baseDir, 3);
        for (HecatonInstance member : restarted) {
            waitUntilRecovered(member);
        }
        assertThat(uuids(restarted)).isEqualTo(uuids);
        assertThat(restarted.get(1).getPartitionService().getPartition(42).getOwner().getUuid()).isEqualTo(ownerOfKey);
        map = restarted.get(2).getMap("persistent");
        assertThat(map.size()).isEqualTo(ENTRIES);
        for (int i = 0; i < ENTRIES; i++) {
            assertThat(map.get(i)).isEqualTo("value" + i);
        }
        // the backups were restored too: the data survives the loss of a member
        restarted.get(0).getLifecycleService().terminate();
        assertTrueEventually(() -> assertThat(restarted.get(1).<Integer, String>getMap("persistent").size())
                .isEqualTo(ENTRIES));
    }

    @Test
    public void indexesAreRebuilt() throws IOException {
        File baseDir = folder.newFolder();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        Config config = config(baseDir);
        config.getMapConfig("persistent").addIndexConfig(new uk.org.hecaton.config.IndexConfig(IndexType.SORTED, "this"));
        HecatonInstance member = factory.newHecatonInstance(config);
        IMap<Integer, Integer> map = member.getMap("persistent");
        for (int i = 0; i < ENTRIES; i++) {
            map.put(i, i);
        }
        member.getCluster().shutdown();

        member = factory.newHecatonInstance(config);
        waitUntilRecovered(member);
        map = member.getMap("persistent");
        assertThat(map.values(Predicates.lessThan("this", 100))).hasSize(100);
        assertThat(map.getLocalMapStats().getIndexStats()).isNotEmpty();
    }

    @Test
    public void cacheRestartsWithItsData() throws IOException {
        File baseDir = folder.newFolder();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(4);
        List<HecatonInstance> members = startMembers(factory, baseDir, 2);
        CacheManager cacheManager = createServerCachingProvider(members.get(0)).getCacheManager();
        CacheConfig<Integer, String> cacheConfig = new CacheConfig<>();
        cacheConfig.setDataPersistenceConfig(new DataPersistenceConfig().setEnabled(true));
        Cache<Integer, String> cache = cacheManager.createCache("persistent-cache", cacheConfig);
        for (int i = 0; i < ENTRIES; i++) {
            cache.put(i, "value" + i);
        }
        for (int i = 0; i < ENTRIES; i += 10) {
            cache.replace(i, "updated" + i);
        }
        for (int i = 1; i < ENTRIES; i += 10) {
            cache.remove(i);
        }
        members.get(0).getCluster().shutdown();

        List<HecatonInstance> restarted = startMembers(factory, baseDir, 2);
        for (HecatonInstance member : restarted) {
            waitUntilRecovered(member);
        }
        Cache<Integer, String> recovered = createServerCachingProvider(restarted.get(1)).getCacheManager()
                .getCache("persistent-cache");
        assertThat(recovered.unwrap(ICache.class).size()).isEqualTo(ENTRIES - ENTRIES / 10);
        assertThat(recovered.get(0)).isEqualTo("updated0");
        assertThat(recovered.get(1)).isNull();
        assertThat(recovered.get(2)).isEqualTo("value2");
    }

    @Test
    public void backupIsRestoredByStartingFromIt() throws IOException {
        File baseDir = folder.newFolder();
        File backupDir = folder.newFolder();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(4);
        List<HecatonInstance> members = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Config config = config(baseDir);
            config.getPersistenceConfig().setBackupDir(backupDir);
            members.add(factory.newHecatonInstance(config));
        }
        IMap<Integer, String> map = members.get(0).getMap("persistent");
        for (int i = 0; i < ENTRIES; i++) {
            map.put(i, "value" + i);
        }
        PersistenceService persistence = members.get(0).getCluster().getPersistenceService();
        assertThat(persistence.isBackupEnabled()).isTrue();
        persistence.backup(42);
        for (HecatonInstance member : members) {
            assertTrueEventually(() -> assertThat(member.getCluster().getPersistenceService().getBackupTaskStatus().getState())
                    .isEqualTo(BackupTaskState.SUCCESS));
        }
        // changes after the backup are not in it
        map.put(-1, "after");
        map.remove(0);
        members.get(0).getCluster().shutdown();

        File backup = new File(backupDir, "backup-42");
        List<HecatonInstance> restored = startMembers(factory, backup, 2);
        for (HecatonInstance member : restored) {
            waitUntilRecovered(member);
        }
        IMap<Integer, String> restoredMap = restored.get(0).getMap("persistent");
        assertThat(restoredMap.size()).isEqualTo(ENTRIES);
        assertThat(restoredMap.get(0)).isEqualTo("value0");
        assertThat(restoredMap.get(-1)).isNull();
    }

    private static Config config(File baseDir) {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "31");
        config.getPersistenceConfig().setEnabled(true).setBaseDir(baseDir)
                .setValidationTimeoutSeconds(60).setDataLoadTimeoutSeconds(60);
        config.addMapConfig(new MapConfig("persistent")
                .setDataPersistenceConfig(new DataPersistenceConfig().setEnabled(true)));
        return config;
    }

    private static List<HecatonInstance> startMembers(TestHecatonInstanceFactory factory, File baseDir, int count) {
        List<HecatonInstance> members = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            members.add(factory.newHecatonInstance(config(baseDir)));
        }
        return members;
    }

    private static Set<UUID> uuids(List<HecatonInstance> members) {
        return members.stream().map(m -> m.getCluster().getLocalMember().getUuid()).collect(Collectors.toSet());
    }

    private static void waitUntilRecovered(HecatonInstance member) {
        assertTrueEventually(() -> assertThat(getNode(member).getNodeExtension().isStartCompleted()).isTrue());
    }
}
