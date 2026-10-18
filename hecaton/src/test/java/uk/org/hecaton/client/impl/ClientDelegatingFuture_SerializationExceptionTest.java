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

package uk.org.hecaton.client.impl;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MapGetCodec;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocationFuture;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.DelegatingCompletableFuture_SerializationExceptionTest;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.sequence.CallIdSequence;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientDelegatingFuture_SerializationExceptionTest
        extends DelegatingCompletableFuture_SerializationExceptionTest {

    private ClientMessage request;
    private ClientMessage response;
    private ILogger logger;
    private SerializationService serializationService;
    private Data key;
    private Data value;
    private ClientInvocationFuture invocationFuture;
    private ClientDelegatingFuture<Object> delegatingFuture;
    private CallIdSequence callIdSequence;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
        key = serializationService.toData("key");
        value = invalidData;
        logger = mock(ILogger.class);
        request = MapGetCodec.encodeRequest("test", key, 1L);
        response = MapGetCodec.encodeResponse(value);
        callIdSequence = mock(CallIdSequence.class);
        invocationFuture = new ClientInvocationFuture(mock(ClientInvocation.class),
                request,
                logger,
                callIdSequence);
        invocationFuture.complete(response);
        delegatingFuture = new ClientDelegatingFuture<>(invocationFuture, serializationService,
                MapGetCodec::decodeResponse, true);
    }

    @Override
    protected InternalCompletableFuture<Object> newCompletableFuture(long completeAfterMillis) {
        return delegatingFuture;
    }

}
