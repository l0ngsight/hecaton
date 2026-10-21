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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DataPersistenceConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.PersistenceClusterDataRecoveryPolicy;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.hotrestart.InternalHotRestartService;
import uk.org.hecaton.internal.management.dto.ClusterHotRestartStatusDTO.ClusterHotRestartStatus;
import uk.org.hecaton.map.IMap;
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static uk.org.hecaton.config.PersistenceClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY;
import static uk.org.hecaton.config.PersistenceClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_RECENT;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class PersistenceRecoveryTest extends HecatonTestSupport {

    private static final int ENTRIES = 1000;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void missingMemberFailsAFullRecoveryAndForceStartStartsTheClusterEmpty() throws IOException {
        File baseDir = folder.newFolder();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(6);
        List<HecatonInstance> members = startClusterWithData(factory, baseDir, FULL_RECOVERY_ONLY);
        members.get(0).getCluster().shutdown();

        List<HecatonInstance> restarted = start(factory, baseDir, FULL_RECOVERY_ONLY, 2);
        assertTrueEventually(() -> assertThat(hotRestart(restarted.get(0)).getCurrentClusterHotRestartStatus()
                .getHotRestartStatus()).isEqualTo(ClusterHotRestartStatus.FAILED));
        assertThat(hotRestart(restarted.get(1)).isStartCompleted()).isFalse();

        assertThat(hotRestart(restarted.get(1)).triggerForceStart()).isTrue();
        for (HecatonInstance member : restarted) {
            waitUntilStarted(member);
        }
        IMap<Integer, String> map = restarted.get(0).getMap("persistent");
        assertThat(map.size()).isZero();
        map.put(1, "new");
        assertThat(restarted.get(1).<Integer, String>getMap("persistent").get(1)).isEqualTo("new");
    }

    @Test
    public void partialStartRecoversWithThePresentMembers() throws IOException {
        File baseDir = folder.newFolder();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(6);
        List<HecatonInstance> members = startClusterWithData(factory, baseDir, PARTIAL_RECOVERY_MOST_RECENT);
        members.get(0).getCluster().shutdown();

        // one replica of every partition is on one of the two members
        List<HecatonInstance> restarted = start(factory, baseDir, PARTIAL_RECOVERY_MOST_RECENT, 2);
        for (HecatonInstance member : restarted) {
            waitUntilStarted(member);
        }
        IMap<Integer, String> map = restarted.get(0).getMap("persistent");
        assertThat(map.size()).isEqualTo(ENTRIES);
        for (int i = 0; i < ENTRIES; i++) {
            assertThat(map.get(i)).isEqualTo("value" + i);
        }
        waitAllForSafeState(restarted);
        // the backups were made again: the data survives the loss of a member
        restarted.get(0).getLifecycleService().terminate();
        assertTrueEventually(() -> assertThat(restarted.get(1).<Integer, String>getMap("persistent").size())
                .isEqualTo(ENTRIES));
    }

    @Test
    public void partialStartCanBeAskedFor() throws IOException {
        File baseDir = folder.newFolder();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(6);
        List<HecatonInstance> members = startClusterWithData(factory, baseDir, FULL_RECOVERY_ONLY);
        members.get(0).getCluster().shutdown();

        List<HecatonInstance> restarted = start(factory, baseDir, FULL_RECOVERY_ONLY, 2);
        assertThat(hotRestart(restarted.get(1)).triggerPartialStart()).isTrue();
        for (HecatonInstance member : restarted) {
            waitUntilStarted(member);
        }
        assertThat(restarted.get(0).getMap("persistent").size()).isEqualTo(ENTRIES);
    }

    @Test
    public void memberRestartingAloneJoinsTheRunningClusterAsANewMember() throws IOException {
        File baseDir = folder.newFolder();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(6);
        List<HecatonInstance> members = startClusterWithData(factory, baseDir, FULL_RECOVERY_ONLY);
        members.get(2).getLifecycleService().terminate();
        waitAllForSafeState(members.subList(0, 2));

        HecatonInstance restarted = factory.newHecatonInstance(config(baseDir, FULL_RECOVERY_ONLY));
        waitUntilStarted(restarted);
        assertClusterSizeEventually(3, restarted);
        waitAllForSafeState(members.get(0), members.get(1), restarted);
        IMap<Integer, String> map = restarted.getMap("persistent");
        assertThat(map.size()).isEqualTo(ENTRIES);
        for (int i = 0; i < ENTRIES; i++) {
            assertThat(map.get(i)).isEqualTo("value" + i);
        }
    }

    private static List<HecatonInstance> startClusterWithData(TestHecatonInstanceFactory factory, File baseDir,
                                                             PersistenceClusterDataRecoveryPolicy policy) {
        List<HecatonInstance> members = start(factory, baseDir, policy, 3);
        assertClusterSizeEventually(3, members);
        waitAllForSafeState(members);
        IMap<Integer, String> map = members.get(0).getMap("persistent");
        for (int i = 0; i < ENTRIES; i++) {
            map.put(i, "value" + i);
        }
        return members;
    }

    private static List<HecatonInstance> start(TestHecatonInstanceFactory factory, File baseDir,
                                               PersistenceClusterDataRecoveryPolicy policy, int count) {
        List<HecatonInstance> members = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            members.add(factory.newHecatonInstance(config(baseDir, policy)));
        }
        return members;
    }

    private static Config config(File baseDir, PersistenceClusterDataRecoveryPolicy policy) {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "31");
        config.setProperty(ClusterProperty.PARTITION_BACKUP_SYNC_INTERVAL.getName(), "1");
        config.getPersistenceConfig().setEnabled(true).setBaseDir(baseDir).setClusterDataRecoveryPolicy(policy)
                .setValidationTimeoutSeconds(5).setDataLoadTimeoutSeconds(60);
        config.addMapConfig(new MapConfig("persistent")
                .setDataPersistenceConfig(new DataPersistenceConfig().setEnabled(true)));
        return config;
    }

    private static InternalHotRestartService hotRestart(HecatonInstance member) {
        return getNode(member).getNodeExtension().getInternalHotRestartService();
    }

    private static void waitUntilStarted(HecatonInstance member) {
        assertTrueEventually(() -> assertThat(getNode(member).getNodeExtension().isStartCompleted()).isTrue());
    }
}
