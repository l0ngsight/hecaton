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

package uk.org.hecaton.internal.management.operation;

import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MCRunConsoleCommandCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCRunScriptCodec;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocationFuture;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.security.AccessControlException;

import static uk.org.hecaton.test.HecatonTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT;
import static uk.org.hecaton.test.HecatonTestSupport.randomString;
import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DisabledOperationsTest {

    private HecatonClientInstanceImpl client;
    private TestHecatonFactory factory;

    @Before
    public void setUp() {
        factory = new TestHecatonFactory();

        // scripting and console are disabled by default
        factory.newHecatonInstance(smallInstanceConfig());
        client = ((HecatonClientProxy) factory.newHecatonClient()).client;
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testRunConsoleCommandIsNotAllowed() throws Exception {
        ClientMessage clientMessage = MCRunConsoleCommandCodec.encodeRequest(randomString(), randomString());
        assertFailure(clientMessage, "Using Console is not allowed on this Hecaton member.");
    }

    @Test
    public void testRunScriptIsNotAllowed() throws Exception {
        ClientMessage clientMessage = MCRunScriptCodec.encodeRequest(randomString(), randomString());
        assertFailure(clientMessage, "Using ScriptEngine is not allowed on this Hecaton member.");
    }

    private void assertFailure(ClientMessage clientMessage, String expectedExceptionMsg) throws Exception {
        ClientInvocation invocation = new ClientInvocation(client, clientMessage, null);
        ClientInvocationFuture future = invocation.invoke();
        assertThatThrownBy(() -> future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS))
                .hasCauseInstanceOf(AccessControlException.class)
                .hasRootCauseMessage(expectedExceptionMsg);
    }

}
