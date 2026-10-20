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

package uk.org.hecaton.client.executor.durable;

import uk.org.hecaton.client.executor.tasks.MapPutRunnable;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.durableexecutor.DurableExecutorService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static uk.org.hecaton.test.HecatonTestSupport.assertSizeEventually;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static uk.org.hecaton.test.HecatonTestSupport.generateKeyOwnedBy;
import static uk.org.hecaton.test.HecatonTestSupport.randomString;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientDurableExecutorServiceExecuteTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    private HecatonInstance server;
    private HecatonInstance client;

    @Before
    public void setup() {
        server = hecatonFactory.newHecatonInstance();
        hecatonFactory.newHecatonInstance();
        hecatonFactory.newHecatonInstance();
        client = hecatonFactory.newHecatonClient();
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testExecute() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        String mapName = randomString();

        service.execute(new MapPutRunnable(mapName));
        IMap map = client.getMap(mapName);

        assertSizeEventually(1, map);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("ConstantConditions")
    public void testExecute_whenTaskNull() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());

        service.execute(null);
    }

    @Test
    public void testExecuteOnKeyOwner() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        String mapName = randomString();

        Member member = server.getCluster().getLocalMember();
        final UUID targetUuid = member.getUuid();
        String key = generateKeyOwnedBy(server);

        service.executeOnKeyOwner(new MapPutRunnable(mapName), key);

        final IMap map = client.getMap(mapName);

        assertTrueEventually(() -> assertTrue(map.containsKey(targetUuid)));
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnKeyOwner_whenKeyNull() {
        DurableExecutorService service = client.getDurableExecutorService(randomString());
        service.executeOnKeyOwner(new MapPutRunnable("map"), null);
    }
}
