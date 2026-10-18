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

package uk.org.hecaton.jet.impl.execution;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nio.Bits;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.config.ProcessingGuarantee;
import uk.org.hecaton.jet.core.AbstractProcessor;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.impl.JobRepository;
import uk.org.hecaton.jet.impl.SnapshotValidationRecord;
import uk.org.hecaton.jet.impl.util.AsyncSnapshotWriterImpl;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.IntSummaryStatistics;
import java.util.stream.IntStream;

import static uk.org.hecaton.jet.core.JobAssertions.assertThat;
import static uk.org.hecaton.jet.core.JobStatus.RUNNING;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.summarizingInt;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SnapshotLargeChunk_IntegrationTest extends JetTestSupport {

    @Test
    public void test_snapshotRestoreLargeChunk() {
        HecatonInstance instance = createHecatonInstance();
        DAG dag = new DAG();
        dag.newVertex("src", LargeStateP::new).localParallelism(1);
        Job job = instance.getJet().newJob(dag, new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(DAYS.toMillis(1)));

        assertThat(job).eventuallyHasStatus(RUNNING);
        job.restart();
        assertThat(job).eventuallyHasStatus(RUNNING);

        // assert that the snapshot exists and that the chunk was large enough
        IMap<Object, Object> map = instance.getMap(JobRepository.snapshotDataMapName(job.getId(), 0));
        SnapshotValidationRecord validationRec = (SnapshotValidationRecord) map.get(SnapshotValidationRecord.KEY);
        assertEquals(1, validationRec.numChunks());
        IntSummaryStatistics stats = map.values().stream()
                                          .filter(v -> v instanceof byte[])
                                          .collect(summarizingInt(v -> ((byte[]) v).length));
        assertTrue("min=" + stats.getMin(), stats.getMin() > AsyncSnapshotWriterImpl.DEFAULT_CHUNK_SIZE);
    }

    /**
     * Processor that saves large value to the key and asserts it when restoring.
     */
    private static final class LargeStateP extends AbstractProcessor {

        private final int[] data = IntStream.range(0, AsyncSnapshotWriterImpl.DEFAULT_CHUNK_SIZE / Bits.INT_SIZE_IN_BYTES)
                                            .toArray();

        @Override
        public boolean complete() {
            return false;
        }

        @Override
        public boolean saveToSnapshot() {
            return tryEmitToSnapshot("key", data);
        }

        @Override
        protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
            assertEquals("key", key);
            assertArrayEquals(data, (int[]) value);
        }
    }
}
