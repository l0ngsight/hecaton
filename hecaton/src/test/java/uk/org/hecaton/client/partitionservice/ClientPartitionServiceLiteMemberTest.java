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

package uk.org.hecaton.client.partitionservice;

import uk.org.hecaton.client.impl.spi.ClientPartitionService;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.TestUtil;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.client.impl.clientside.ClientTestUtil.getHecatonClientInstanceImpl;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientPartitionServiceLiteMemberTest {

    private TestHecatonFactory factory;

    @Before
    public void before() {
        factory = new TestHecatonFactory();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testWithOnlyLiteMember() {
        factory.newHecatonInstance(new Config().setLiteMember(true));

        HecatonInstance client = factory.newHecatonClient();
        ClientPartitionService clientPartitionService = getClientPartitionService(client);
        assertTrueEventually(() -> assertEquals(271, clientPartitionService.getPartitionCount()));
        assertNull(clientPartitionService.getPartitionOwner(0));

    }

    @Test
    public void testWithLiteMemberAndDataMember() {
        HecatonInstance hecatonInstance = factory.newHecatonInstance();
        factory.newHecatonInstance(new Config().setLiteMember(true));

        TestUtil.warmUpPartitions(hecatonInstance);
        HecatonInstance client = factory.newHecatonClient();
        ClientPartitionService clientPartitionService = getClientPartitionService(client);
        assertTrueEventually(() -> {
            assertNotEquals(0, clientPartitionService.getPartitionCount());
            assertNotNull(clientPartitionService.getPartitionOwner(0));
        });
    }

    private ClientPartitionService getClientPartitionService(HecatonInstance client) {
        return getHecatonClientInstanceImpl(client).getClientPartitionService();
    }

}
