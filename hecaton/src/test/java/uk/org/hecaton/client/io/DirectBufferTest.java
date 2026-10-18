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

package uk.org.hecaton.client.io;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static uk.org.hecaton.spi.properties.ClusterProperty.SOCKET_BUFFER_DIRECT;
import static uk.org.hecaton.spi.properties.ClusterProperty.SOCKET_CLIENT_BUFFER_DIRECT;
import static org.junit.Assert.assertArrayEquals;

/**
 * A test that verifies that the Client can deal with various permutations of direct-buffers.
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class DirectBufferTest extends HecatonTestSupport {

    @Parameters()
    public static Collection<Boolean[]> params() {
        return Arrays.asList(new Boolean[][]{
                {false, false},
                {false, true},
                {true, false},
                {true, true},
        });
    }

    @Parameter(0)
    public boolean memberDirectBuffer;

    @Parameter(1)
    public boolean clientDirectBuffer;

    private HecatonInstance client;
    private HecatonInstance server;

    @After
    public void after() {
        if (client != null) {
            client.shutdown();
        }
        if (server != null) {
            server.shutdown();
        }
        Hecaton.shutdownAll();
        HecatonClient.shutdownAll();
    }

    @Test
    public void test() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        config.setProperty(SOCKET_BUFFER_DIRECT.getName(), String.valueOf(memberDirectBuffer));

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(SOCKET_CLIENT_BUFFER_DIRECT.getName(), String.valueOf(clientDirectBuffer));

        server = Hecaton.newHecatonInstance(config);
        client = HecatonClient.newHecatonClient(clientConfig);

        List<byte[]> values = new LinkedList<>();

        IMap<Integer, byte[]> map = client.getMap("foo");
        for (int k = 0; k < 24; k++) {
            byte[] value = randomByteArray((int) Math.pow(2, k));
            values.add(value);
            map.put(k, value);
        }

        for (int k = 0; k < values.size(); k++) {
            byte[] expected = values.get(k);
            assertArrayEquals(expected, map.get(k));
        }
    }

    private static byte[] randomByteArray(int length) {
        byte[] bytes = new byte[length];
        new Random().nextBytes(bytes);
        return bytes;
    }
}
