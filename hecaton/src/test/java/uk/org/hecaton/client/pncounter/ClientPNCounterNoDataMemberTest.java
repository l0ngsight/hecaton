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
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.ConsistencyLostException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.crdt.pncounter.PNCounter;
import uk.org.hecaton.internal.crdt.pncounter.AbstractPNCounterNoDataMemberTest;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Client implementation for testing behaviour of {@link ConsistencyLostException}
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientPNCounterNoDataMemberTest extends AbstractPNCounterNoDataMemberTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    private String counterName = randomMapName("counter-");
    private HecatonInstance client;

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Override
    protected void mutate(PNCounter driver) {
        driver.addAndGet(5);
    }

    @Override
    protected PNCounter getCounter() {
        hecatonFactory.newHecatonInstance(new Config().setLiteMember(true));
        client = hecatonFactory.newHecatonClient();
        return client.getPNCounter(counterName);
    }
}
