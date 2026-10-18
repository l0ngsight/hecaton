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

import uk.org.hecaton.client.impl.ClientDelegatingFuture;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.protocol.codec.MCRunScriptCodec;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RunScriptOperationTest extends HecatonTestSupport {

    // Let's use Groovy on classpath, because Azul Zulu 6-7 doesn't include the JavaScript engine (Rhino)
    // and Nashorn was removed from JDK 15+
    private static final String GROOVY_ENGINE = "groovy";

    private HecatonInstance hz;
    private TestHecatonFactory factory;
    private HecatonClientInstanceImpl client;

    @Before
    public void setUp() {
        factory = new TestHecatonFactory();

        Config config = smallInstanceConfig();
        config.getManagementCenterConfig().setScriptingEnabled(true);
        config.setInstanceName(randomString());
        hz = factory.newHecatonInstance(config);
        client = ((HecatonClientProxy) factory.newHecatonClient()).client;
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testScripting_Groovy() throws Exception {
        ClientDelegatingFuture<Object> future = runScript(client, hz, GROOVY_ENGINE, "hecaton.getName()");
        Object actual = future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertThat(actual).isEqualTo(hz.getName());
    }

    @Test
    public void testScripting_Groovy_ReturnsNothing() throws Exception {
        ClientDelegatingFuture<Object> future = runScript(client, hz, GROOVY_ENGINE, "for(int i = 0; i<5; i++) {}");
        Object actual = future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertThat(actual).isNull();
    }

    @Test
    public void testScripting_CouldNotFindScriptEngine() throws Exception {
        String engine = "pseudocode";
        ClientDelegatingFuture<Object> future = runScript(client, hz, engine, randomString());

        assertThatThrownBy(() -> future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Could not find ScriptEngine named '" + engine + "'."
                        + " Please add the corresponding ScriptEngine to the classpath of this Hecaton member");
    }

    private ClientDelegatingFuture<Object> runScript(HecatonClientInstanceImpl client, HecatonInstance member, String engine, String script) {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCRunScriptCodec.encodeRequest(engine, script),
                null,
                member.getCluster().getLocalMember().getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                client.getSerializationService(),
                MCRunScriptCodec::decodeResponse
        );
    }
}
