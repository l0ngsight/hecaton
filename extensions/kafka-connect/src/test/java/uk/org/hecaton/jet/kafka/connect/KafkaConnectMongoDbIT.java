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
package uk.org.hecaton.jet.kafka.connect;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.config.ProcessingGuarantee;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.core.JobStatus;
import uk.org.hecaton.jet.datamodel.Tuple2;
import uk.org.hecaton.jet.impl.JobRepository;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.StreamSource;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.NightlyTest;
import uk.org.hecaton.test.annotation.SlowTest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MongoDBContainer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.jet.TestedVersions.MONGO_IMAGE;
import static uk.org.hecaton.jet.core.JobAssertions.assertThat;
import static uk.org.hecaton.jet.kafka.connect.TestUtil.getConnectorURL;
import static uk.org.hecaton.test.DockerTestUtil.assumeDockerEnabled;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonSerialClassRunner.class)
@Category({SlowTest.class, NightlyTest.class})
public class KafkaConnectMongoDbIT extends JetTestSupport {
    private static MongoDBContainer mongoContainer;
    private static MongoClient mongoClient;

    @BeforeClass
    public static void setupClass() {
        assumeDockerEnabled();
        mongoContainer = new MongoDBContainer(MONGO_IMAGE);
        mongoContainer.start();
        mongoClient = MongoClients.create(mongoContainer.getConnectionString());
    }

    @Test
    public void clusterMemberDown() throws InterruptedException {
        Pipeline p = Pipeline.create();
        p.readFrom(streamSource("test"))
         .withIngestionTimestamps()
         .map(e -> Tuple2.tuple2(e, e))
         .writeTo(Sinks.map("test"))
         .getPipeline();


        Config config = smallInstanceConfig();
        config.setProperty("hecaton.logging.type", "log4j2");
        config.getJetConfig().setResourceUploadEnabled(true);
        config.addMapConfig(new MapConfig("*").setBackupCount(2));
        HecatonInstance[] hecatonInstances = createHecatonInstances(config, 3);

        HecatonInstance leader = hecatonInstances[0];
        HecatonInstance nonLeader = hecatonInstances[2];

        var job = leader.getJet().newJob(p, jobConfig());
        var jobRepository = new JobRepository(leader);
        assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);
        waitForNextSnapshot(jobRepository, job.getId(), 30, false);

        //Start creating new docs
        AtomicInteger recordsCreatedCounter = new AtomicInteger();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        ScheduledFuture<?> scheduledFuture = scheduler.scheduleWithFixedDelay(() -> createOneRecord("test",
                recordsCreatedCounter.incrementAndGet()), 0, 10, MILLISECONDS);

        // wait for some snapshots and data
        SECONDS.sleep(3);
        waitForNextSnapshot(jobRepository, job.getId(), 30, false);

        Map<String, String> map = nonLeader.getMap("test");
        final int currentElements = map.size();
        System.out.printf("Created docs before scale down: %04d%n", recordsCreatedCounter.get());
        System.out.printf("Map size before scale down: %04d%n", currentElements);
        leader.shutdown();

        assertTrueEventually(() -> {
            Job j = nonLeader.getJet().getJob(job.getId());
            assert j != null;
            assertThat(j.getStatus()).isEqualTo(JobStatus.RUNNING);
        });

        // Wait for new elements
        assertTrueEventually(() -> assertThat(map).hasSizeGreaterThan(currentElements));

        scheduledFuture.cancel(false);

        assertTrueEventually(() -> assertThat(map).hasSize(recordsCreatedCounter.intValue()));
    }

    public JobConfig jobConfig() {
        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(TestUtil.class);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        jobConfig.setSnapshotIntervalMillis(500);
        jobConfig.addJar(getConnectorURL("mongo-kafka-connect-1.16.0-all.jar"));
        return jobConfig;
    }

    public StreamSource<?> streamSource(String testName) {
        Properties connectorProperties = new Properties();
        connectorProperties.setProperty("name", "mongo");
        connectorProperties.setProperty("connector.class", "com.mongodb.kafka.connect.MongoSourceConnector");
        connectorProperties.setProperty("connection.uri", mongoContainer.getConnectionString());
        connectorProperties.setProperty("database", testName);
        connectorProperties.setProperty("collection", testName);
        connectorProperties.setProperty("topic.prefix", "mongo");
        return KafkaConnectSources.connect(connectorProperties, TestUtil::convertToStringWithJustIndexForMongo);
    }

    public void createOneRecord(String testName, int index) {
        Document document = new Document().append("index", index);
        mongoClient.getDatabase(testName).getCollection(testName).insertOne(document);
    }

}
