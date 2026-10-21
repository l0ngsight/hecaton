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

package uk.org.hecaton.jet.core;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.JobStateSnapshot;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.config.ProcessingGuarantee;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.test.TestSources;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exported snapshots and job upgrades (F18): a running job's state is exported under a name, and
 * a new job (an upgraded version, for example) starts from it.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class ExportedSnapshotTest extends HecatonTestSupport {

    @Test
    public void exportedSnapshotStartsTheUpgradedJob() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setEnabled(true);
        HecatonInstance member = createHecatonInstance(config);
        IMap<Long, Long> results = member.getMap("results");

        Job job = member.getJet().newJob(countingPipeline(), jobConfig());
        assertTrueEventually(() -> assertThat(results.size()).isGreaterThan(10));

        JobStateSnapshot exported = job.exportSnapshot("while-running");
        assertThat(exported.name()).isEqualTo("while-running");
        assertThat(member.getJet().getJobStateSnapshot("while-running")).isNotNull();
        assertThat(job.getStatus()).isEqualTo(JobStatus.RUNNING);

        JobStateSnapshot terminal = job.cancelAndExportSnapshot("before-upgrade");
        assertThat(terminal.jobId()).isEqualTo(job.getId());
        assertTrueEventually(() -> assertThat(job.getStatus()).isEqualTo(JobStatus.FAILED));
        int countAtUpgrade = results.size();

        // the upgraded job continues from the exported state
        Job upgraded = member.getJet().newJob(countingPipeline(),
                jobConfig().setInitialSnapshotName("before-upgrade"));
        assertTrueEventually(() -> assertThat(upgraded.getStatus()).isEqualTo(JobStatus.RUNNING));
        assertTrueEventually(() -> assertThat(results.size()).isGreaterThan(countAtUpgrade + 10));
        // the sequence continues from the snapshot instead of starting from 0 again
        assertThat(results.get(0L)).isNotNull();
        upgraded.cancel();
    }

    private static Pipeline countingPipeline() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(50))
                .withoutTimestamps()
                .map(event -> Map.entry(event.sequence(), event.timestamp()))
                .writeTo(Sinks.map("results"));
        return pipeline;
    }

    private static JobConfig jobConfig() {
        return new JobConfig().setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE).setSnapshotIntervalMillis(1000);
    }
}
