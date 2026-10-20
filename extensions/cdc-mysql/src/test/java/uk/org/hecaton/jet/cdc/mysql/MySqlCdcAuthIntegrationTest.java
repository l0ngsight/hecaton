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

package uk.org.hecaton.jet.cdc.mysql;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.JetException;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.cdc.ChangeRecord;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.StreamSource;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

@Category(NightlyTest.class)
public class MySqlCdcAuthIntegrationTest extends AbstractMySqlCdcIntegrationTest {

    @Test
    public void wrongPassword() {
        StreamSource<ChangeRecord> source = MySqlCdcSources.mysql("name")
                .setDatabaseAddress(mysql.getHost())
                .setDatabasePort(mysql.getMappedPort(MYSQL_PORT))
                .setDatabaseUser("debezium")
                .setDatabasePassword("wrongPassword")
                .setClusterName("dbserver1")
                .build();

        Pipeline pipeline = pipeline(source);

        // when
        HecatonInstance hz = createHecatonInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);

        // then
        assertThatThrownBy(job::join)
                .hasCauseInstanceOf(JetException.class)
                .hasStackTraceContaining("Access denied for user");
    }

    private Pipeline pipeline(StreamSource<ChangeRecord> source) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        return pipeline;
    }
}
