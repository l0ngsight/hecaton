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
import uk.org.hecaton.client.impl.clientside.ClientTestUtil;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.client.test.executor.tasks.CancellationAwareTask;
import uk.org.hecaton.client.test.executor.tasks.FailingCallable;
import uk.org.hecaton.client.test.executor.tasks.SelectNoMembers;
import uk.org.hecaton.client.test.executor.tasks.SerializedCounterCallable;
import uk.org.hecaton.client.test.executor.tasks.TaskWithUnserializableResponse;
import uk.org.hecaton.cluster.MemberSelector;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.core.ExecutionCallback;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static uk.org.hecaton.test.HecatonTestSupport.assertOpenEventually;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static uk.org.hecaton.test.HecatonTestSupport.ignore;
import static uk.org.hecaton.test.HecatonTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExecutorServiceTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();
    private HecatonInstance client;
    private HecatonInstance instance;

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

        instance = hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);
        client = hecatonFactory.newHecatonClient(clientConfig);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalExecutorStats() {
        IExecutorService service = client.getExecutorService(randomString());

        service.getLocalExecutorStats();
    }

    @Test
    public void testIsTerminated() {
        IExecutorService service = client.getExecutorService(randomString());

        assertFalse(service.isTerminated());
    }

    @Test
    public void testIsShutdown() {
        IExecutorService service = client.getExecutorService(randomString());

        assertFalse(service.isShutdown());
    }

    @Test
    public void testShutdownNow() {
        final IExecutorService service = client.getExecutorService(randomString());

        service.shutdownNow();

        assertTrueEventually(() -> assertTrue(service.isShutdown()));
    }

    @Test
    public void testShutdownMultipleTimes() {
        final IExecutorService service = client.getExecutorService(randomString());
        service.shutdownNow();
        service.shutdown();

        assertTrueEventually(() -> assertTrue(service.isShutdown()));
    }

    @Test(expected = TimeoutException.class)
    public void testCancellationAwareTask_whenTimeOut()
            throws InterruptedException, ExecutionException, TimeoutException {
        IExecutorService service = client.getExecutorService(randomString());
        CancellationAwareTask task = new CancellationAwareTask(Long.MAX_VALUE);

        Future<Boolean> future = service.submit(task);

        future.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testFutureAfterCancellationAwareTaskTimeOut()
            throws InterruptedException, ExecutionException {
        IExecutorService service = client.getExecutorService(randomString());
        CancellationAwareTask task = new CancellationAwareTask(Long.MAX_VALUE);

        Future<Boolean> future = service.submit(task);

        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException ignored) {
        }

        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast/issues/4677")
    //Ignored because fixing it requires extensive refactoring see ClientExecutorServiceCancelTest
    public void testCancelFutureAfterCancellationAwareTaskTimeOut()
            throws InterruptedException, ExecutionException {
        IExecutorService service = client.getExecutorService(randomString());
        CancellationAwareTask task = new CancellationAwareTask(Long.MAX_VALUE);

        Future<Boolean> future = service.submit(task);

        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException ignored) {
        }

        assertTrue(future.cancel(true));
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }

    @Test(expected = CancellationException.class)
    public void testGetFutureAfterCancel()
            throws InterruptedException, ExecutionException {
        IExecutorService service = client.getExecutorService(randomString());
        CancellationAwareTask task = new CancellationAwareTask(Long.MAX_VALUE);

        Future<Boolean> future = service.submit(task);
        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException ignored) {
        }
        future.cancel(true);

        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void testSubmitFailingCallableException()
            throws ExecutionException, InterruptedException {
        IExecutorService service = client.getExecutorService(randomString());
        Future<String> failingFuture = service.submit(new FailingCallable());

        failingFuture.get();
    }

    @Test
    public void testSubmitFailingCallableException_withExecutionCallback() {
        IExecutorService service = client.getExecutorService(randomString());
        final CountDownLatch latch = new CountDownLatch(1);
        service.submit(new FailingCallable(), new ExecutionCallback<>() {
            @Override
            public void onResponse(String response) {
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
            }
        });
        assertOpenEventually(latch);
    }

    @Test(expected = IllegalStateException.class)
    public void testSubmitFailingCallableReasonExceptionCause()
            throws Throwable {
        IExecutorService service = client.getExecutorService(randomString());
        Future<String> failingFuture = service.submit(new FailingCallable());

        try {
            failingFuture.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test(expected = RejectedExecutionException.class)
    public void testExecute_withNoMemberSelected() {
        IExecutorService service = client.getExecutorService(randomString());
        String mapName = randomString();
        MemberSelector selector = new SelectNoMembers();

        service.execute(new MapPutRunnable(mapName), selector);
    }

    @Test
    public void testCallableSerializedOnce()
            throws ExecutionException, InterruptedException {
        String name = randomString();
        IExecutorService service = client.getExecutorService(name);
        SerializedCounterCallable counterCallable = new SerializedCounterCallable();
        Future<Integer> future = service.submitToKeyOwner(counterCallable, name);
        assertEquals(Integer.valueOf(2), future.get());
    }

    @Test
    public void testCallableSerializedOnce_submitToAddress()
            throws ExecutionException, InterruptedException {
        String name = randomString();
        IExecutorService service = client.getExecutorService(name);
        SerializedCounterCallable counterCallable = new SerializedCounterCallable();
        Future<Integer> future = service.submitToMember(counterCallable, instance.getCluster().getLocalMember());
        assertEquals(Integer.valueOf(2), future.get());
    }

    @Test(expected = HecatonSerializationException.class)
    public void testUnserializableResponse_exceptionPropagatesToClient()
            throws Throwable {
        IExecutorService service = client.getExecutorService("executor");
        TaskWithUnserializableResponse counterCallable = new TaskWithUnserializableResponse();
        Future<Object> future = service.submit(counterCallable);
        try {
            future.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test(expected = HecatonSerializationException.class)
    public void testUnserializableResponse_exceptionPropagatesToClientCallback()
            throws Throwable {
        IExecutorService service = client.getExecutorService("executor");
        TaskWithUnserializableResponse counterCallable = new TaskWithUnserializableResponse();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> throwable = new AtomicReference<>();
        service.submit(counterCallable, new ExecutionCallback<>() {
            @Override
            public void onResponse(Object response) {

            }

            @Override
            public void onFailure(Throwable t) {
                throwable.set(t.getCause());
                countDownLatch.countDown();
            }
        });
        assertOpenEventually(countDownLatch);
        throw throwable.get();
    }

    @Ignore("Behaviour needs a better test, due to switching from user executor to ForkJoinPool.commonPool")
    @Test
    public void testExecutionCallback_whenExecutionRejected() {
        final AtomicReference<Throwable> exceptionThrown = new AtomicReference<>();
        final CountDownLatch waitForShutdown = new CountDownLatch(1);
        final CountDownLatch didShutdown = new CountDownLatch(1);
        final HecatonClientInstanceImpl hzClient = ClientTestUtil.getHecatonClientInstanceImpl(client);

        ExecutionRejectedRunnable.waitForShutdown = waitForShutdown;
        ExecutionRejectedRunnable.didShutdown = didShutdown;

        IExecutorService executorService = hzClient.getExecutorService("executor");
        Thread t = new Thread(() -> {
            try {
                waitForShutdown.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                ignore(e);
            }
            ForkJoinPool.commonPool().shutdown();
            didShutdown.countDown();
        });
        t.start();
        executorService.submit(new ExecutionRejectedRunnable(), new ExecutionCallback<>() {
            @Override
            public void onResponse(Object response) {
            }

            @Override
            public void onFailure(Throwable t) {
                exceptionThrown.set(t);
            }
        });

        // assert a RejectedExecutionException is thrown from already shutdown user executor
        // it is not wrapped in a HecatonClientNotActiveException because the client is still running
        assertTrueEventually(() -> assertTrue(exceptionThrown.get() instanceof RejectedExecutionException));
    }

    private static class ExecutionRejectedRunnable implements Runnable, Serializable {
        static CountDownLatch waitForShutdown;
        static CountDownLatch didShutdown;

        @Override
        public void run() {
            waitForShutdown.countDown();
            try {
                didShutdown.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                ignore(e);
            }
        }
    }
}
