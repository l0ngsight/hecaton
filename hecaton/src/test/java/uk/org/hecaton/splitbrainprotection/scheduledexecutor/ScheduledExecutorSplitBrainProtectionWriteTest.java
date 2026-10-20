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

package uk.org.hecaton.splitbrainprotection.scheduledexecutor;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.scheduledexecutor.IScheduledExecutorService;
import uk.org.hecaton.scheduledexecutor.IScheduledFuture;
import uk.org.hecaton.splitbrainprotection.AbstractSplitBrainProtectionTest;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionException;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionOn;
import uk.org.hecaton.test.ChangeLoggingRule;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.splitbrainprotection.scheduledexecutor.ScheduledExecutorSplitBrainProtectionWriteTest.ExecRunnable.callable;
import static uk.org.hecaton.splitbrainprotection.scheduledexecutor.ScheduledExecutorSplitBrainProtectionWriteTest.ExecRunnable.runnable;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ScheduledExecutorSplitBrainProtectionWriteTest extends AbstractSplitBrainProtectionTest {

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-debug-scheduledexecutor.xml");

    @Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{SplitBrainProtectionOn.WRITE}, {SplitBrainProtectionOn.READ_WRITE}});
    }

    @Parameter
    public static SplitBrainProtectionOn splitBrainProtectionOn;

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(smallInstanceConfig(), new TestHecatonInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void schedule_runnable_splitBrainProtection() throws Exception {
        exec(0).schedule(runnable(), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void schedule_runnable_noSplitBrainProtection() throws Exception {
        exec(3).schedule(runnable(), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void schedule_callable_splitBrainProtection() throws Exception {
        exec(0).schedule(callable(), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void schedule_callable_noSplitBrainProtection() throws Exception {
        exec(3).schedule(callable(), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleAtFixedRate_callable_splitBrainProtection() {
        exec(0).scheduleAtFixedRate(runnable(), 10, 10, TimeUnit.MILLISECONDS).cancel(false);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void scheduleAtFixedRate_callable_noSplitBrainProtection() throws Exception {
        exec(3).scheduleAtFixedRate(runnable(), 10, 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleOnMember_runnable_splitBrainProtection() throws Exception {
        exec(0).scheduleOnMember(runnable(), member(0), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void scheduleOnMember_runnable_noSplitBrainProtection() throws Exception {
        exec(3).scheduleOnMember(runnable(), member(3), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleOnMember_callable_splitBrainProtection() throws Exception {
        exec(0).scheduleOnMember(callable(), member(0), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void scheduleOnMember_callable_noSplitBrainProtection() throws Exception {
        exec(3).scheduleOnMember(callable(), member(3), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleOnMemberAtFixedRate_runnable_splitBrainProtection() {
        exec(0).scheduleOnMemberAtFixedRate(runnable(), member(0), 10, 10, TimeUnit.MILLISECONDS).cancel(false);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void scheduleOnMemberAtFixedRate_runnable_noSplitBrainProtection() throws Exception {
        exec(3).scheduleOnMemberAtFixedRate(runnable(), member(3), 10, 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleOnKeyOwner_runnable_splitBrainProtection() throws Exception {
        exec(0).scheduleOnKeyOwner(runnable(), key(0), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void scheduleOnKeyOwner_runnable_noSplitBrainProtection() throws Exception {
        exec(3).scheduleOnKeyOwner(runnable(), key(3), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleOnKeyOwner_callable_splitBrainProtection() throws Exception {
        exec(0).scheduleOnKeyOwner(callable(), key(0), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void scheduleOnKeyOwner_callable_noSplitBrainProtection() throws Exception {
        exec(3).scheduleOnKeyOwner(callable(), key(3), 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleOnKeyOwnerAtFixedRate_runnable_splitBrainProtection() {
        exec(0).scheduleOnKeyOwnerAtFixedRate(runnable(), key(0), 10, 10, TimeUnit.MILLISECONDS).cancel(false);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void scheduleOnKeyOwnerAtFixedRate_runnable_noSplitBrainProtection() throws Exception {
        exec(3).scheduleOnKeyOwnerAtFixedRate(runnable(), key(3), 10, 10, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void scheduleOnAllMembers_runnable_splitBrainProtection() throws Exception {
        wait(exec(0).scheduleOnAllMembers(runnable(), 10, TimeUnit.MILLISECONDS));
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void scheduleOnAllMembers_runnable_noSplitBrainProtection() throws Exception {
        wait(exec(3).scheduleOnAllMembers(runnable(), 10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void scheduleOnAllMembers_callable_splitBrainProtection() throws Exception {
        wait(exec(0).scheduleOnAllMembers(callable(), 10, TimeUnit.MILLISECONDS));
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void scheduleOnAllMembers_callable_noSplitBrainProtection() throws Exception {
        wait(exec(3).scheduleOnAllMembers(callable(), 10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void scheduleOnAllMembersAtFixedRate_runnable_splitBrainProtection() {
        cancel(exec(0).scheduleOnAllMembersAtFixedRate(runnable(), 10, 10, TimeUnit.MILLISECONDS));
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void scheduleOnAllMembersAtFixedRate_runnable_noSplitBrainProtection() throws Exception {
        wait(exec(3).scheduleOnAllMembersAtFixedRate(runnable(), 10, 10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void scheduleOnMembers_runnable_splitBrainProtection() throws Exception {
        wait(exec(0).scheduleOnMembers(runnable(), singletonList(member(0)), 10, TimeUnit.MILLISECONDS));
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void scheduleOnMembers_runnable_noSplitBrainProtection() throws Exception {
        wait(exec(3).scheduleOnMembers(runnable(), singletonList(member(3)), 10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void scheduleOnMembers_callable_splitBrainProtection() throws Exception {
        Map<Member, IScheduledFuture<String>> futures = exec(0)
                .scheduleOnMembers(callable(), singletonList(member(0)),
                        10, TimeUnit.MILLISECONDS);
        wait(futures);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void scheduleOnMembers_callable_noSplitBrainProtection() throws Exception {
        Map<Member, IScheduledFuture<String>> futures = exec(3)
                .scheduleOnMembers(callable(), singletonList(member(3)), 10, TimeUnit.MILLISECONDS);
        wait(futures);
    }

    @Test
    public void scheduleOnMembersAtFixedRate_runnable_splitBrainProtection() {
        cancel(exec(0).scheduleOnMembersAtFixedRate(runnable(), singletonList(member(0)), 10, 10, TimeUnit.MILLISECONDS));
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void scheduleOnMembersAtFixedRate_runnable_noSplitBrainProtection() throws Exception {
        wait(exec(3).scheduleOnMembersAtFixedRate(runnable(), singletonList(member(3)), 10, 10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void shutdown_splitBrainProtection() {
        exec(0, "shutdown").shutdown();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void shutdown_noSplitBrainProtection() {
        exec(3, "shutdown").shutdown();
    }

    @SuppressWarnings("unused")
    static class ExecRunnable implements Runnable, Callable<String>, Serializable {
        @Override
        public String call() throws Exception {
            return "response";
        }

        @Override
        public void run() {
        }

        public static Runnable runnable() {
            return new ExecRunnable();
        }

        public static Callable<String> callable() {
            return new ExecRunnable();
        }
    }

    private Object key(int index) {
        return generateKeyOwnedBy(cluster.getInstance(index), true);
    }

    private Member member(int index) {
        return getNodeEngineImpl(cluster.getInstance(index)).getLocalMember();
    }

    private <V> void wait(Map<Member, IScheduledFuture<V>> futures) throws Exception {
        for (IScheduledFuture f : futures.values()) {
            f.get();
        }
    }

    private <V> void cancel(Map<Member, IScheduledFuture<V>> futures) {
        for (IScheduledFuture f : futures.values()) {
            f.cancel(false);
        }
    }

    protected IScheduledExecutorService exec(int index) {
        return exec(index, "");
    }

    protected IScheduledExecutorService exec(int index, String postfix) {
        return scheduledExec(index, splitBrainProtectionOn, postfix);
    }
}
