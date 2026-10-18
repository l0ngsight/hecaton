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

package uk.org.hecaton.client.pncounter;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.CRDTReplicationConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.PNCounterConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.crdt.pncounter.PNCounter;
import uk.org.hecaton.internal.crdt.pncounter.AbstractPNCounterBasicIntegrationTest;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;

/**
 * Client implementation for basic
 * {@link PNCounter} integration tests
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientPNCounterBasicIntegrationTest extends AbstractPNCounterBasicIntegrationTest {

    @Parameters(name = "replicaCount:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {1},
                {2},
                {Integer.MAX_VALUE},
        });
    }

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    private HecatonInstance[] members;
    private HecatonInstance[] clients;

    @Parameter
    public int replicaCount;

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void setup() {
        final PNCounterConfig counterConfig = new PNCounterConfig()
                .setName("default")
                .setReplicaCount(replicaCount)
                .setStatisticsEnabled(true);
        final Config config = new Config()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "5")
                .setCRDTReplicationConfig(new CRDTReplicationConfig()
                        .setReplicationPeriodMillis(200)
                        .setMaxConcurrentReplicationTargets(Integer.MAX_VALUE))
                .addPNCounterConfig(counterConfig);
        members = hecatonFactory.newInstances(config, 2);
        clients = new HecatonInstance[]{hecatonFactory.newHecatonClient(), hecatonFactory.newHecatonClient()};
    }

    @Override
    protected HecatonInstance getInstance1() {
        return clients[0];
    }

    @Override
    protected HecatonInstance getInstance2() {
        return clients[1];
    }

    @Override
    protected HecatonInstance[] getMembers() {
        return members;
    }
}
