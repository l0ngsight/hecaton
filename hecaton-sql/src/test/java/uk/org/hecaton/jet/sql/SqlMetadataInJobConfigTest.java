/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.jet.sql;

import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.JobStatus;
import uk.org.hecaton.jet.impl.JetClientInstanceImpl;
import uk.org.hecaton.jet.impl.JobAndSqlSummary;
import uk.org.hecaton.jet.sql.impl.connector.test.TestBatchSqlConnector;
import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.sql.SqlStatement;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static uk.org.hecaton.jet.config.JobConfigArguments.KEY_SQL_QUERY_TEXT;
import static uk.org.hecaton.jet.config.JobConfigArguments.KEY_SQL_UNBOUNDED;
import static uk.org.hecaton.jet.core.JobAssertions.assertThat;
import static uk.org.hecaton.jet.core.JobStatus.COMPLETED;
import static uk.org.hecaton.jet.core.JobStatus.RUNNING;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HecatonSerialClassRunner.class)
public class SqlMetadataInJobConfigTest extends SqlTestSupport {
    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(1, null, null);
    }

    @Test
    public void test_selectMetadata_member() {
        String sql = "SELECT * FROM table(generate_stream(1))";
        try (SqlResult ignored = client().getSql().execute(new SqlStatement(sql).setCursorBufferSize(1))) {
            waitForJobRunning();
            List<Job> runningJobs = getJobsByStatus(RUNNING);
            JobConfig config = runningJobs.get(0).getConfig();
            assertEquals(sql, config.getArgument(KEY_SQL_QUERY_TEXT));
            assertEquals(Boolean.TRUE, config.getArgument(KEY_SQL_UNBOUNDED));
        }
    }

    @Test
    public void test_selectMetadata_client() {
        String sql = "SELECT * FROM table(generate_stream(1))";
        try (SqlResult ignored = client().getSql().execute(new SqlStatement(sql).setCursorBufferSize(1))) {
            waitForJobRunning();
            List<Job> runningJobs = getJobsByStatus(RUNNING);
            JobConfig config = runningJobs.get(0).getConfig();
            assertEquals(sql, config.getArgument(KEY_SQL_QUERY_TEXT));
            assertEquals(Boolean.TRUE, config.getArgument(KEY_SQL_UNBOUNDED));
        }
    }

    @Test
    public void test_selectMetadata_clientJobSummary() {
        String sql = "SELECT * FROM table(generate_stream(1))";
        try (SqlResult ignored = client().getSql().execute(new SqlStatement(sql).setCursorBufferSize(1))) {
            waitForJobRunning();
            JetClientInstanceImpl jet = (JetClientInstanceImpl) client().getJet();
            List<JobAndSqlSummary> jobSummaries = jet.getJobAndSqlSummaryList().stream()
                                        .filter(jobSummary -> jobSummary.getStatus() == RUNNING)
                                        .collect(toList());

            JobAndSqlSummary jobSummary = jobSummaries.get(0);
            assertNotNull(jobSummary.getSqlSummary());
            assertEquals(sql, jobSummary.getSqlSummary().getQuery());
            assertEquals(Boolean.TRUE, jobSummary.getSqlSummary().isUnbounded());
        }
    }

    @Test
    @SuppressWarnings("resource")
    public void test_createBatchJobMetadata() {
        TestBatchSqlConnector.create(instance().getSql(), "src", 3);
        createMapping("dest", Integer.class, String.class);

        String sql = "CREATE JOB testJob AS INSERT INTO dest SELECT v * 2, 'value-' || v FROM src WHERE v < 2";
        instance().getSql().execute(sql);
        Job job = instance().getJet().getJob("testJob");
        assertThat(job).eventuallyHasStatus(COMPLETED);

        List<Job> completedJobs = getJobsByStatus(COMPLETED);
        assertEquals(1, completedJobs.size());
        JobConfig config = completedJobs.get(0).getConfig();
        assertEquals(sql, config.getArgument(KEY_SQL_QUERY_TEXT));
        assertEquals(Boolean.FALSE, config.getArgument(KEY_SQL_UNBOUNDED));
        assertEquals(Boolean.FALSE, config.isSuspendOnFailure());
    }

    @Test
    @SuppressWarnings("resource")
    public void test_createStreamingJobMetadata() {
        createMapping("dest", Integer.class, String.class);

        String sql = "CREATE JOB testJob AS INSERT INTO dest SELECT v * 2, 'value-' || v FROM TABLE(generate_stream(1))";
        instance().getSql().execute(sql);
        awaitSingleRunningJob(instance());

        List<Job> runningJobs = getJobsByStatus(RUNNING);
        assertEquals(1, runningJobs.size());
        JobConfig config = runningJobs.get(0).getConfig();
        assertEquals(sql, config.getArgument(KEY_SQL_QUERY_TEXT));
        assertEquals(Boolean.TRUE, config.getArgument(KEY_SQL_UNBOUNDED));
        assertEquals(Boolean.TRUE, config.isSuspendOnFailure());
    }

    @Test
    public void test_dmlMetadata() {
        createMapping("dest", Integer.class, Integer.class);
        TestBatchSqlConnector.create(instance().getSql(), "src", 1, true);

        String sql = "INSERT INTO dest SELECT v, v FROM src";
        spawn(() -> instance().getSql().execute(sql));
        awaitSingleRunningJob(instance());

        List<Job> runningJobs = getJobsByStatus(RUNNING);
        assertEquals(1, runningJobs.size());
        JobConfig config = runningJobs.get(0).getConfig();
        assertEquals(sql, config.getArgument(KEY_SQL_QUERY_TEXT));
        assertEquals(Boolean.TRUE, config.getArgument(KEY_SQL_UNBOUNDED));
    }

    private void waitForJobRunning() {
        assertTrueEventually(() -> assertEquals(1, getJobsByStatus(RUNNING).size()));
    }

    private List<Job> getJobsByStatus(JobStatus status) {
        return instance().getJet().getJobs().stream()
                .filter(job -> job.getStatus() == status)
                .collect(toList());
    }
}
