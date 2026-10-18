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

package uk.org.hecaton.internal.crdt.pncounter;

import uk.org.hecaton.config.CRDTReplicationConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.ConsistencyLostException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.crdt.pncounter.PNCounter;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * Member implementation for testing behaviour of {@link ConsistencyLostException}
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PNCounterConsistencyLostTest extends AbstractPNCounterConsistencyLostTest {

    private HecatonInstance[] members;
    private HecatonInstance liteMember;

    @Before
    public void setup() {
        final Config dataConfig = new Config()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "5")
                .setCRDTReplicationConfig(new CRDTReplicationConfig()
                        .setReplicationPeriodMillis(Integer.MAX_VALUE)
                        .setMaxConcurrentReplicationTargets(Integer.MAX_VALUE));
        final Config liteConfig = new Config()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "5")
                .setLiteMember(true);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        members = factory.newInstances(dataConfig, 2);
        liteMember = factory.newHecatonInstance(liteConfig);
    }


    @Override
    protected HecatonInstance[] getMembers() {
        return members;
    }

    @Override
    protected Address getCurrentTargetReplicaAddress(PNCounter driver) {
        return ((PNCounterProxy) driver).getCurrentTargetReplicaAddress();
    }

    @Override
    protected void assertState(PNCounter driver) {
        assertEquals(5, driver.get());
    }

    @Override
    protected void mutate(PNCounter driver) {
        driver.addAndGet(5);
    }

    @Override
    protected PNCounter getCounter() {
        final PNCounter counter = liteMember.getPNCounter("counter");
        ((PNCounterProxy) counter).setOperationTryCount(1);
        return counter;
    }
}
