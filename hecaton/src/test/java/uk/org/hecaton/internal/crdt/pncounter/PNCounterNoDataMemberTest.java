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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.crdt.pncounter.PNCounter;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test behaviour of PN counter when there are no data members in the cluster
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PNCounterNoDataMemberTest extends AbstractPNCounterNoDataMemberTest {

    private HecatonInstance liteMember;
    private String counterName = randomMapName("counter-");

    @Before
    public void setup() {
        final Config liteConfig = new Config()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "5")
                .setLiteMember(true);

        liteMember = createHecatonInstanceFactory().newHecatonInstance(liteConfig);
    }

    @Override
    protected void mutate(PNCounter driver) {
        driver.addAndGet(100);
    }

    @Override
    protected PNCounter getCounter() {
        final PNCounter counter = liteMember.getPNCounter(counterName);
        ((PNCounterProxy) counter).setOperationTryCount(1);
        return counter;
    }
}
