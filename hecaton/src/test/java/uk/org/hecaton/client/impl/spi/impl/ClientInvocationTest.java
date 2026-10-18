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

package uk.org.hecaton.client.impl.spi.impl;

import uk.org.hecaton.client.HecatonClientNotActiveException;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MapSizeCodec;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.OperationTimeoutException;
import uk.org.hecaton.internal.util.FutureUtil;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientInvocationTest extends ClientTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }


    /**
     * When an async operation fails because of a node termination,
     * failure stack trace is copied incrementally for each async invocation/future
     * <p/>
     * see https://github.com/hazelcast/hazelcast/issues/4192
     */
    @Test
    public void executionCallback_TooLongThrowableStackTrace() {
        HecatonInstance server = hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();
        IMap<Object, Object> map = client.getMap(randomMapName());

        DummyEntryProcessor ep = new DummyEntryProcessor();

        int count = 100;
        FailureExecutionCallback[] callbacks = new FailureExecutionCallback[count];
        String key = randomString();
        for (int i = 0; i < count; i++) {
            callbacks[i] = new FailureExecutionCallback();
            map.submitToKey(key, ep).whenCompleteAsync(callbacks[i]);
        }

        // crash the server
        getNode(server).getServer().shutdown();
        server.getLifecycleService().terminate();

        int callBackCount = 0;
        for (FailureExecutionCallback callback : callbacks) {
            callBackCount++;
            assertOpenEventually("Callback should be notified on time! callbackCount:" + callBackCount, callback.latch);

            Throwable failure = callback.failure;
            if (failure == null) {
                continue;
            }
            int stackTraceLength = failure.getStackTrace().length;
            assertTrue("Failure stack trace should not be too long! Current: "
                    + stackTraceLength, stackTraceLength < 50);

            Throwable cause = failure.getCause();
            if (cause == null) {
                continue;
            }
            stackTraceLength = cause.getStackTrace().length;
            assertTrue("Cause stack trace should not be too long! Current: "
                    + stackTraceLength, stackTraceLength < 50);
        }
    }

    @Test
    public void executionCallback_FailOnShutdown() {
        HecatonInstance server = hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig()
                .getConnectionRetryConfig().setClusterConnectTimeoutMillis(10000);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        final CountDownLatch disconnectedLatch = new CountDownLatch(1);

        IMap<Object, Object> map = client.getMap(randomName());
        client.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED) {
                disconnectedLatch.countDown();
            }
        });
        server.shutdown();
        assertOpenEventually(disconnectedLatch);
        int n = 100;
        final CountDownLatch errorLatch = new CountDownLatch(n);
        CompletableFuture[] userCallbackFutures = new CompletableFuture[n];
        for (int i = 0; i < n; i++) {
            // submitStage is completed with HecatonClientNotActiveException
            CompletionStage<Object> submitStage = map.submitToKey(randomString(), new DummyEntryProcessor());
            // a user-supplied callback submitted to default executor will not be executed with RejectedExecutionException
            CompletableFuture<Object> userCallbackFuture = submitStage.whenCompleteAsync((v, t) -> fail("This must not be executed")).toCompletableFuture();
            userCallbackFutures[i] = userCallbackFuture;
            userCallbackFuture.whenCompleteAsync((v, t) -> {
                if (t instanceof HecatonClientNotActiveException) {
                    errorLatch.countDown();
                } else {
                    throw rethrow(t);
                }
            });
        }
        FutureUtil.waitWithDeadline(Arrays.asList(userCallbackFutures), 30, TimeUnit.SECONDS, t -> {
            // t is ExecutionException < HecatonClientNotActiveException
            if (t.getCause() == null
                    || !(t.getCause() instanceof HecatonClientNotActiveException)) {
                System.out.println("Throwable was unexpected instance of "
                        + (t.getCause() == null ? t.getClass() : t.getCause().getClass()));
                throw rethrow(t);
            }
        });
        assertOpenEventually("Not all of the requests failed", errorLatch);
    }

    private static class DummyEntryProcessor implements EntryProcessor<Object, Object, Object> {
        @Override
        public Object process(Map.Entry<Object, Object> entry) {
            LockSupport.parkNanos(10000);
            return null;
        }

        @Override
        public EntryProcessor<Object, Object, Object> getBackupProcessor() {
            return null;
        }
    }


    private static class FailureExecutionCallback implements BiConsumer<Object, Throwable> {
        final CountDownLatch latch = new CountDownLatch(1);
        volatile Throwable failure;

        @Override
        public void accept(Object o, Throwable throwable) {
            if (throwable != null) {
                failure = throwable;
            }
            latch.countDown();
        }
    }

    @Test
    public void invokeOnPartitionOwnerRedirectsToRandom_WhenPartitionOwnerIsnull() throws Exception {
        hecatonFactory.newHecatonInstance();
        HecatonClientInstanceImpl client = getHecatonClientInstanceImpl(hecatonFactory.newHecatonClient());

        ClientMessage request = MapSizeCodec.encodeRequest("test");
        int ownerlessPartition = 4000;
        ClientInvocation invocation = new ClientInvocation(client, request, "map", ownerlessPartition);
        assertEquals(0, MapSizeCodec.decodeResponse(invocation.invoke().get()));
    }

    @Test
    public void invokeOnMemberRedirectsToRandom_whenMemberIsNotInMemberList() throws Exception {
        hecatonFactory.newHecatonInstance();
        UUID unavailableTarget = UUID.randomUUID();
        HecatonClientInstanceImpl client = getHecatonClientInstanceImpl(hecatonFactory.newHecatonClient());

        ClientMessage request = MapSizeCodec.encodeRequest("test");
        ClientInvocation invocation = new ClientInvocation(client, request, "map", unavailableTarget);
        assertEquals(0, MapSizeCodec.decodeResponse(invocation.invoke().get()));
    }

    @Test(expected = OperationTimeoutException.class)
    public void invokeOnPartitionOwner_redirectDisallowedToRandom_WhenPartitionOwnerIsnull() {
        hecatonFactory.newHecatonInstance();
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "1");
        HecatonClientInstanceImpl client = getHecatonClientInstanceImpl(hecatonFactory.newHecatonClient(config));

        ClientMessage request = MapSizeCodec.encodeRequest("test");
        int ownerlessPartition = 4000;
        ClientInvocation invocation = new ClientInvocation(client, request, "map", ownerlessPartition);
        invocation.disallowRetryOnRandom();
        invocation.invoke().joinInternal();
    }

    @Test(expected = OperationTimeoutException.class)
    public void invokeOnMember_redirectDisallowedToRandom_whenMemberIsNotInMemberList() {
        hecatonFactory.newHecatonInstance();
        UUID unavailableTarget = UUID.randomUUID();
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "1");
        HecatonClientInstanceImpl client = getHecatonClientInstanceImpl(hecatonFactory.newHecatonClient(config));

        ClientMessage request = MapSizeCodec.encodeRequest("test");
        ClientInvocation invocation = new ClientInvocation(client, request, "map", unavailableTarget);
        invocation.disallowRetryOnRandom();
        invocation.invoke().joinInternal();
    }
}
