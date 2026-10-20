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

import uk.org.hecaton.client.impl.ClientDelegatingFuture;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MapGetCodec;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.CompletableFutureAbstractTest;
import uk.org.hecaton.test.ExpectedRuntimeException;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static uk.org.hecaton.client.impl.clientside.ClientTestUtil.getHecatonClientInstanceImpl;
import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static uk.org.hecaton.test.HecatonTestSupport.sleepAtLeastMillis;
import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;

public class ClientDelegatingFutureTest_CompletionStageTest extends CompletableFutureAbstractTest {
    /*
     * This test sets up a member & client, as required for
     * construction of ClientInvocation. However invocations are not
     * sent to the member, as we need to complete invocations normally or
     * exceptionally depending on test case.
     */

    private TestHecatonFactory factory;
    private HecatonClientInstanceImpl client;
    private ClientMessage request;
    private ClientMessage response;
    private SerializationService serializationService;
    private Data key;
    private Data value;
    private ClientInvocation invocation;

    @Before
    public void setup() {
        factory = new TestHecatonFactory();
        factory.newHecatonInstance(getConfig());
        client = getHecatonClientInstanceImpl(factory.newHecatonClient());
        serializationService = new DefaultSerializationServiceBuilder().build();
        key = serializationService.toData("key");
        value = serializationService.toData(returnValue);
        request = MapGetCodec.encodeRequest("test", key, 1L);
        response = MapGetCodec.encodeResponse(value);
        invocation = new ClientInvocation(client, request, "test");
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return config;
    }

    @Override
    protected CompletableFuture<Object> newCompletableFuture(boolean exceptional, long completeAfterMillis) {
        invocation.getCallIdSequence().next();
        ClientInvocationFuture cf = invocation.getClientInvocationFuture();

        ClientDelegatingFuture<Object> future =
                new ClientDelegatingFuture<>(cf, serializationService, MapGetCodec::decodeResponse);

        Executor completionExecutor;
        if (completeAfterMillis <= 0) {
            completionExecutor = CALLER_RUNS;
        } else {
            completionExecutor = command -> new Thread(() -> {
                sleepAtLeastMillis(completeAfterMillis);
                command.run();
            }, "test-completion-thread").start();
        }
        if (exceptional) {
            completionExecutor.execute(() -> invocation.completeExceptionally(new ExpectedRuntimeException()));
        } else {
            completionExecutor.execute(completeNormally(invocation));
        }
        return future;
    }

    private Runnable completeNormally(ClientInvocation future) {
        return () -> future.complete(response);
    }

}
