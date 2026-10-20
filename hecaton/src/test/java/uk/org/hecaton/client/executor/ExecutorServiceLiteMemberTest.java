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
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import static uk.org.hecaton.cluster.memberselector.MemberSelectors.LITE_MEMBER_SELECTOR;
import static uk.org.hecaton.executor.ExecutorServiceTestSupport.CountingDownExecutionCallback;
import static uk.org.hecaton.executor.ExecutorServiceTestSupport.LocalMemberReturningCallable;
import static uk.org.hecaton.executor.ExecutorServiceTestSupport.ResultHoldingMultiExecutionCallback;
import static uk.org.hecaton.executor.ExecutorServiceTestSupport.ResultSettingRunnable;
import static uk.org.hecaton.executor.ExecutorServiceTestSupport.assertOpenEventually;
import static uk.org.hecaton.executor.ExecutorServiceTestSupport.assertTrueEventually;
import static uk.org.hecaton.executor.ExecutorServiceTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecutorServiceLiteMemberTest {

    private TestHecatonFactory factory;

    private final String defaultMemberConfigXml = "hecaton-test-executor.xml";
    private Config liteConfig = new XmlConfigBuilder(getClass().getClassLoader().getResourceAsStream(defaultMemberConfigXml))
            .build().setLiteMember(true);
    private Config config = new XmlConfigBuilder(getClass().getClassLoader().getResourceAsStream(defaultMemberConfigXml)).build();
    private ClientConfig clientConfig = new XmlClientConfigBuilder("classpath:hecaton-client-test-executor.xml").build();

    public ExecutorServiceLiteMemberTest()
            throws IOException {
    }

    private HecatonInstance newHecatonInstance() {
        return factory.newHecatonInstance(config);
    }

    private HecatonInstance newHecatonLiteMember() {
        return factory.newHecatonInstance(liteConfig);
    }

    private HecatonInstance newHecatonClient() {
        return factory.newHecatonClient(clientConfig);
    }

    @Before
    public void before() {
        factory = new TestHecatonFactory();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test(expected = RejectedExecutionException.class)
    public void test_executeRunnable_failsWhenNoLiteMemberExists() {
        newHecatonInstance();
        final HecatonInstance client = newHecatonClient();

        final String name = randomString();
        final IExecutorService executor = client.getExecutorService(name);
        executor.execute(new ResultSettingRunnable(name), LITE_MEMBER_SELECTOR);
    }

    @Test
    public void test_executeRunnable_onLiteMember() {
        final HecatonInstance lite1 = newHecatonLiteMember();
        final HecatonInstance lite2 = newHecatonLiteMember();
        newHecatonInstance();
        final HecatonInstance client = newHecatonClient();

        final String name = randomString();
        final IExecutorService executor = client.getExecutorService(name);
        executor.execute(new ResultSettingRunnable(name), LITE_MEMBER_SELECTOR);

        assertTrueEventually(() -> {
            final IMap<Object, Object> results = lite1.getMap(name);
            assertEquals(1, results.size());
            final boolean executedOnLite1 = results.containsKey(lite1.getCluster().getLocalMember());
            final boolean executedOnLite2 = results.containsKey(lite2.getCluster().getLocalMember());

            assertTrue(executedOnLite1 || executedOnLite2);
        });
    }

    @Test
    public void test_executeRunnable_onAllLiteMembers() {
        final HecatonInstance lite1 = newHecatonLiteMember();
        final HecatonInstance lite2 = newHecatonLiteMember();
        newHecatonInstance();
        final HecatonInstance client = newHecatonClient();

        final String name = randomString();
        final IExecutorService executor = client.getExecutorService(name);
        executor.executeOnMembers(new ResultSettingRunnable(name), LITE_MEMBER_SELECTOR);

        assertTrueEventually(() -> {
            final IMap<Object, Object> results = lite1.getMap(name);
            assertEquals(2, results.size());
            assertTrue(results.containsKey(lite1.getCluster().getLocalMember()));
            assertTrue(results.containsKey(lite2.getCluster().getLocalMember()));
        });
    }

    @Test(expected = RejectedExecutionException.class)
    public void test_submitCallable_failsWhenNoLiteMemberExists() {
        newHecatonInstance();
        final HecatonInstance client = newHecatonClient();

        final IExecutorService executor = client.getExecutorService(randomString());
        executor.submit(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR);
    }

    @Test
    public void test_submitCallable_onLiteMember()
            throws ExecutionException, InterruptedException {
        final HecatonInstance lite1 = newHecatonLiteMember();
        final HecatonInstance lite2 = newHecatonLiteMember();
        newHecatonInstance();
        final HecatonInstance client = newHecatonClient();

        final IExecutorService executor = client.getExecutorService(randomString());
        final Future<Member> future = executor.submit(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR);
        final Member executedMember = future.get();

        final boolean executedOnLite1 = lite1.getCluster().getLocalMember().equals(executedMember);
        final boolean executedOnLite2 = lite2.getCluster().getLocalMember().equals(executedMember);
        assertTrue(executedOnLite1 || executedOnLite2);
    }

    @Test
    public void test_submitCallable_onLiteMembers()
            throws ExecutionException, InterruptedException {
        final HecatonInstance lite1 = newHecatonLiteMember();
        final HecatonInstance lite2 = newHecatonLiteMember();
        newHecatonInstance();
        final HecatonInstance client = newHecatonClient();

        final IExecutorService executor = client.getExecutorService(randomString());
        final Map<Member, Future<Member>> results = executor
                .submitToMembers(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR);

        assertEquals(2, results.size());

        final Member liteMember1 = lite1.getCluster().getLocalMember();
        final Member liteMember2 = lite2.getCluster().getLocalMember();

        assertEquals(liteMember1, results.get(liteMember1).get());
        assertEquals(liteMember2, results.get(liteMember2).get());
    }

    @Test
    public void test_submitCallableWithCallback_toLiteMember() {
        final HecatonInstance lite1 = newHecatonLiteMember();
        final HecatonInstance lite2 = newHecatonLiteMember();
        newHecatonInstance();
        final HecatonInstance client = newHecatonClient();

        final CountingDownExecutionCallback<Member> callback = new CountingDownExecutionCallback<>(1);
        final IExecutorService executor = client.getExecutorService(randomString());
        executor.submit(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR, callback);

        assertOpenEventually(callback.getLatch());
        final Object result = callback.getResult();
        final boolean executedOnLite1 = lite1.getCluster().getLocalMember().equals(result);
        final boolean executedOnLite2 = lite2.getCluster().getLocalMember().equals(result);
        assertTrue(executedOnLite1 || executedOnLite2);
    }

    @Test
    public void test_submitCallableWithCallback_toLiteMembers() {
        final HecatonInstance lite1 = newHecatonLiteMember();
        final HecatonInstance lite2 = newHecatonLiteMember();
        newHecatonInstance();
        final HecatonInstance client = newHecatonClient();

        final ResultHoldingMultiExecutionCallback callback = new ResultHoldingMultiExecutionCallback();
        final IExecutorService executor = client.getExecutorService(randomString());
        executor.submitToMembers(new LocalMemberReturningCallable(), LITE_MEMBER_SELECTOR, callback);

        assertTrueEventually(() -> {
            final Map<Member, Object> results = callback.getResults();

            assertNotNull(results);
            final Member liteMember1 = lite1.getCluster().getLocalMember();
            final Member liteMember2 = lite2.getCluster().getLocalMember();
            assertEquals(liteMember1, results.get(liteMember1));
            assertEquals(liteMember2, results.get(liteMember2));
        });
    }

}
