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

package uk.org.hecaton.client.executor;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.XmlClientConfigBuilder;
import uk.org.hecaton.client.executor.tasks.MapPutRunnable;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.client.test.executor.tasks.SelectAllMembers;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.MemberSelector;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static uk.org.hecaton.test.HecatonTestSupport.assertSizeEventually;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static uk.org.hecaton.test.HecatonTestSupport.generateKeyOwnedBy;
import static uk.org.hecaton.test.HecatonTestSupport.randomString;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExecutorServiceExecuteTest {

    private static final int CLUSTER_SIZE = 3;
    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();
    private HecatonInstance server1;
    private HecatonInstance server2;
    private HecatonInstance client;

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void setup()
            throws IOException {
        Config config = new XmlConfigBuilder(getClass().getClassLoader().getResourceAsStream("hecaton-test-executor.xml"))
                .build();
        ClientConfig clientConfig = new XmlClientConfigBuilder("classpath:hecaton-client-test-executor.xml").build();

        server1 = hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);
        server2 = hecatonFactory.newHecatonInstance(config);
        client = hecatonFactory.newHecatonClient(clientConfig);
    }

    @Test
    public void testExecute() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();

        service.execute(new MapPutRunnable(mapName));
        IMap map = client.getMap(mapName);

        assertSizeEventually(1, map);
    }

    @Test
    public void testExecute_withMemberSelector() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();
        MemberSelector selector = new SelectAllMembers();

        service.execute(new MapPutRunnable(mapName), selector);
        IMap map = client.getMap(mapName);

        assertSizeEventually(1, map);
    }

    @Test(expected = NullPointerException.class)
    public void testExecute_whenTaskNull() {
        IExecutorService service = client.getExecutorService(randomString());

        service.execute(null);
    }

    @Test
    public void testExecuteOnKeyOwner() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();

        Member member = server1.getCluster().getLocalMember();
        final UUID targetUuid = member.getUuid();
        String key = generateKeyOwnedBy(server1);

        service.executeOnKeyOwner(new MapPutRunnable(mapName), key);

        final IMap map = client.getMap(mapName);

        assertTrueEventually(() -> assertTrue(map.containsKey(targetUuid)));
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnKeyOwner_whenKeyNull() {
        IExecutorService service = client.getExecutorService(randomString());
        service.executeOnKeyOwner(new MapPutRunnable("map"), null);
    }

    @Test
    public void testExecuteOnMember() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();

        Member member = server1.getCluster().getLocalMember();
        final UUID targetUuid = member.getUuid();

        service.executeOnMember(new MapPutRunnable(mapName), member);

        final IMap map = client.getMap(mapName);

        assertTrueEventually(() -> assertTrue(map.containsKey(targetUuid)));
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnMember_WhenMemberNull() {
        IExecutorService service = client.getExecutorService(randomString());

        service.executeOnMember(new MapPutRunnable("map"), null);
    }

    @Test
    public void testExecuteOnMembers() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();
        Collection<Member> collection = new ArrayList<>();
        final Member member1 = server1.getCluster().getLocalMember();
        final Member member2 = server2.getCluster().getLocalMember();
        collection.add(member1);
        collection.add(member2);

        service.executeOnMembers(new MapPutRunnable(mapName), collection);

        final IMap map = client.getMap(mapName);
        assertTrueEventually(() -> {
            assertTrue(map.containsKey(member1.getUuid()));
            assertTrue(map.containsKey(member2.getUuid()));
        });
    }

    @Test
    public void testExecuteOnMembers_withEmptyCollection() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();
        Collection<Member> collection = new ArrayList<>();

        service.executeOnMembers(new MapPutRunnable(mapName), collection);

        IMap map = client.getMap(mapName);
        assertSizeEventually(0, map);
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnMembers_WhenCollectionNull() {
        IExecutorService service = client.getExecutorService(randomString());
        Collection<Member> collection = null;

        service.executeOnMembers(new MapPutRunnable("task"), collection);
    }

    @Test
    public void testExecuteOnMembers_withSelector() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();
        MemberSelector selector = new SelectAllMembers();

        service.executeOnMembers(new MapPutRunnable(mapName), selector);

        IMap map = client.getMap(mapName);
        assertSizeEventually(CLUSTER_SIZE, map);
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnMembers_whenSelectorNull() {
        IExecutorService service = client.getExecutorService(randomString());
        MemberSelector selector = null;

        service.executeOnMembers(new MapPutRunnable("task"), selector);
    }

    @Test
    public void testExecuteOnAllMembers() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();

        service.executeOnAllMembers(new MapPutRunnable(mapName));

        IMap map = client.getMap(mapName);
        assertSizeEventually(CLUSTER_SIZE, map);
    }
}
