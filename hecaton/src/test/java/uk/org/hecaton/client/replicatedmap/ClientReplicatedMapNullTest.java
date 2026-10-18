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

package uk.org.hecaton.client.replicatedmap;

import uk.org.hecaton.client.impl.proxy.ClientReplicatedMapProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.replicatedmap.AbstractReplicatedMapNullTest;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Client implementation for basic replicated map methods nullability tests
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientReplicatedMapNullTest extends AbstractReplicatedMapNullTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();
    private HecatonInstance client;

    @Before
    public void setup() {
        hecatonFactory.newHecatonInstance();
        client = hecatonFactory.newHecatonClient();
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testPutAllWithMetadata_nullability() {
        assertThrowsNPE(m -> ((ClientReplicatedMapProxy) m).putAllWithMetadataAsync(null));
    }

    @Override
    protected HecatonInstance getDriver() {
        return client;
    }
}
