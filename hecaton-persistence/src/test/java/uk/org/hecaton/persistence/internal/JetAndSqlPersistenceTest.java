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
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.config.ProcessingGuarantee;
import uk.org.hecaton.jet.core.JobStatus;
import uk.org.hecaton.jet.impl.JetServiceBackend;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.test.TestSources;
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

import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Jet lossless restart (F18) and SQL catalog persistence (F19): Jet's job records, snapshots and
 * results and the SQL catalog survive a cluster restart.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class JetAndSqlPersistenceTest extends HecatonTestSupport {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void streamingJobResumesAfterClusterRestart() throws IOException {
        File baseDir = folder.newFolder();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance member = factory.newHecatonInstance(config(baseDir));

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(50))
                .withoutTimestamps()
                .map(event -> java.util.Map.entry(event.sequence(), event.timestamp()))
                .writeTo(Sinks.map("results"));
        Job job = member.getJet().newJob(pipeline, new JobConfig().setName("counting")
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE).setSnapshotIntervalMillis(200));
        IMap<Long, Long> results = member.getMap("results");
        assertTrueEventually(() -> assertThat(results.size()).isGreaterThan(20));
        member.getCluster().shutdown();

        member = factory.newHecatonInstance(config(baseDir));
        HecatonInstance restarted = member;
        assertTrueEventually(() -> assertThat(getNode(restarted).getNodeExtension().isStartCompleted()).isTrue());
        // the master finds the job in the persisted job records
        assertTrueEventually(() -> assertThat(restarted.getJet().getJob(job.getId())).isNotNull());
        Job resumed = restarted.getJet().getJob(job.getId());
        assertThat(resumed.getName()).isEqualTo("counting");
        assertTrueEventually(() -> assertThat(resumed.getStatus()).isEqualTo(JobStatus.RUNNING));
        IMap<Long, Long> resumedResults = restarted.getMap("results");
        int before = resumedResults.size();
        assertThat(before).isGreaterThan(20);
        assertTrueEventually(() -> assertThat(resumedResults.size()).isGreaterThan(before + 20));
    }

    @Test
    public void sqlCatalogSurvivesClusterRestart() throws IOException {
        File baseDir = folder.newFolder();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance member = factory.newHecatonInstance(config(baseDir));
        // the SQL module writes mappings, views and types to the catalog map
        member.getMap(JetServiceBackend.SQL_CATALOG_MAP_NAME).put("mapping", "CREATE MAPPING m TYPE IMap");
        member.getCluster().shutdown();

        member = factory.newHecatonInstance(config(baseDir));
        HecatonInstance restarted = member;
        assertTrueEventually(() -> assertThat(getNode(restarted).getNodeExtension().isStartCompleted()).isTrue());
        assertThat(restarted.getMap(JetServiceBackend.SQL_CATALOG_MAP_NAME).get("mapping"))
                .isEqualTo("CREATE MAPPING m TYPE IMap");
    }

    @Test
    public void losslessRestartNeedsPersistence() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setEnabled(true).setLosslessRestartEnabled(true);
        assertThatThrownBy(() -> createHecatonInstance(config)).isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("needs persistence");
    }

    @Test
    public void sqlCatalogPersistenceNeedsPersistence() {
        Config config = smallInstanceConfig();
        config.getSqlConfig().setCatalogPersistenceEnabled(true);
        assertThatThrownBy(() -> createHecatonInstance(config)).isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("needs persistence");
    }

    private static Config config(File baseDir) {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "31");
        config.getPersistenceConfig().setEnabled(true).setBaseDir(baseDir)
                .setValidationTimeoutSeconds(60).setDataLoadTimeoutSeconds(60);
        config.getJetConfig().setEnabled(true).setLosslessRestartEnabled(true);
        config.getSqlConfig().setCatalogPersistenceEnabled(true);
        config.getMapConfig("results").getDataPersistenceConfig().setEnabled(true);
        return config;
    }
}
