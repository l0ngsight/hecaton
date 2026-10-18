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

package uk.org.hecaton.jet;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.config.JetConfig;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.lang.Math.max;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

/**
 * Extends {@link JetTestSupport} in such a way that one cluster is used for
 * all tests in the class.
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@SuppressWarnings("checkstyle:declarationorder")
public abstract class TestInClusterSupport extends JetTestSupport {

    protected static final String JOURNALED_MAP_PREFIX = "journaledMap.";
    protected static final String JOURNALED_CACHE_PREFIX = "journaledCache.";
    protected static final int MEMBER_COUNT = 2;
    protected static final int JOURNAL_CAPACITY_PER_PARTITION = 1000;

    protected static TestHecatonFactory factory = new TestHecatonFactory();
    private static HecatonInstance[] allHecatonInstances;

    // these variables are read and updated by different threads
    protected static volatile HecatonInstance member;
    protected static volatile HecatonInstance client;

    protected static final TestMode MEMBER_TEST_MODE = new TestMode("member", () -> member);
    protected static final TestMode CLIENT_TEST_MODE = new TestMode("client", () -> client);
    protected static int parallelism;

    @Parameter
    public TestMode testMode;

    @Parameters(name = "{index}: mode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(MEMBER_TEST_MODE, CLIENT_TEST_MODE);
    }

    @BeforeClass
    public static void setupCluster() {
        member = createCluster(MEMBER_COUNT, prepareConfig());
        client = factory.newHecatonClient();
    }

    protected static Config prepareConfig() {
        parallelism = Runtime.getRuntime().availableProcessors() / MEMBER_COUNT / 2;
        Config config = smallInstanceConfig();

        JetConfig jetConfig = config.getJetConfig().setResourceUploadEnabled(true);
        jetConfig.setCooperativeThreadCount(max(2, parallelism));
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        // Set partition count to match the parallelism of IMap sources.
        // Their preferred local parallelism is 2, therefore partition count
        // should be 2 * MEMBER_COUNT.
        int partitionCount = 2 * MEMBER_COUNT;
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(partitionCount));
        config.addCacheConfig(new CacheSimpleConfig().setName("*"));
        config.getMapConfig(JOURNALED_MAP_PREFIX + '*')
                .getEventJournalConfig()
                .setCapacity(partitionCount * JOURNAL_CAPACITY_PER_PARTITION)
                .setEnabled(true);
        config.getCacheConfig(JOURNALED_CACHE_PREFIX + '*')
                .getEventJournalConfig()
                .setCapacity(partitionCount * JOURNAL_CAPACITY_PER_PARTITION)
                .setEnabled(true);
        return config;
    }

    @AfterClass
    public static void tearDown() throws Exception {
        spawn(() -> factory.terminateAll())
                .get(1, TimeUnit.MINUTES);

        factory = null;
        allHecatonInstances = null;
        member = null;
        client = null;
    }

    @After
    public void after() throws Exception {
        Future<?> future = spawn(() ->
                cleanUpCluster(allHecatonInstances()));
        future.get(1, TimeUnit.MINUTES);
    }

    protected HecatonInstance hz() {
        return testMode.getHecatonInstance();
    }

    protected Job execute(Pipeline p, JobConfig config) {
        Job job = hz().getJet().newJob(p, config);
        job.join();
        return job;
    }

    protected static HecatonInstance[] allHecatonInstances() {
        return allHecatonInstances;
    }

    private static HecatonInstance createCluster(int nodeCount, Config config) {
        factory = new TestHecatonFactory();
        allHecatonInstances = new HecatonInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            allHecatonInstances[i] = factory.newHecatonInstance(config);
        }
        return allHecatonInstances[0];
    }

    protected static final class TestMode {

        private final String name;
        private final Supplier<HecatonInstance> getHecatonInstanceFn;

        TestMode(String name, Supplier<HecatonInstance> getHecatonInstanceFn) {
            this.name = name;
            this.getHecatonInstanceFn = getHecatonInstanceFn;
        }

        public HecatonInstance getHecatonInstance() {
            return getHecatonInstanceFn.get();
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
