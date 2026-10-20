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

package uk.org.hecaton.client.map.impl.querycache;

import uk.org.hecaton.client.impl.proxy.ClientMapProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.impl.querycache.NodeQueryCacheContextTest.QuerySchedulerRepetitionTask;
import uk.org.hecaton.map.impl.querycache.NodeQueryCacheContextTest.QuerySchedulerTask;
import uk.org.hecaton.map.impl.querycache.QueryCacheContext;
import uk.org.hecaton.map.impl.querycache.QueryCacheScheduler;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheContextTest extends HecatonTestSupport {

    private TestHecatonFactory factory;

    private QueryCacheContext context;

    @Before
    public void setUp() {
        factory = new TestHecatonFactory();

        factory.newHecatonInstance();

        HecatonInstance hz = factory.newHecatonClient();
        ClientMapProxy proxy = (ClientMapProxy) hz.getMap("test");
        context = proxy.getQueryCacheContext();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDestroy() {
        context.destroy();
    }

    @Test
    public void testGetMemberList() {
        Collection<Member> memberList = context.getMemberList();

        assertNotNull(memberList);
        assertEquals(1, memberList.size());
    }

    @Test
    public void testPartitionId() {
        int partitionId = context.getPartitionId("myKey");

        assertTrue(partitionId >= 0);
    }

    @Test
    public void testGetQueryCacheScheduler() {
        QueryCacheScheduler scheduler = context.getQueryCacheScheduler();
        assertNotNull(scheduler);

        final QuerySchedulerTask task = new QuerySchedulerTask();
        scheduler.execute(task);

        final QuerySchedulerRepetitionTask repetitionTask = new QuerySchedulerRepetitionTask();
        scheduler.scheduleWithRepetition(repetitionTask, 1);

        assertTrueEventually(() -> {
            assertTrue(task.executed);
            assertTrue(repetitionTask.counter.get() > 1);
        });

        scheduler.shutdown();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetPublisherContext() {
        context.getPublisherContext();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetThisNodesAddress() {
        context.getThisNodesAddress();
    }

    @Test(expected = NullPointerException.class)
    public void testInvokerWrapper_invokeOnTarget_whenAddressIsNull_thenThrowException() {
        context.getInvokerWrapper().invokeOnTarget(new Object(), null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokerWrapper_executeOperation() {
        context.getInvokerWrapper().executeOperation(null);
    }
}
