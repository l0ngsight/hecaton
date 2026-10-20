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

package uk.org.hecaton.internal.diagnostics;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.NetworkConfig;
import uk.org.hecaton.config.TcpIpConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nio.Packet;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.operation.GetOperation;
import uk.org.hecaton.spi.impl.operationservice.impl.DummyOperation;
import uk.org.hecaton.spi.impl.operationservice.impl.operations.Backup;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;

/**
 * This test can't run with mocked Hecaton instances, since we rely on a real TcpIpConnectionManager.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class OverloadedConnectionsPluginTest extends AbstractDiagnosticsPluginTest {

    private HecatonInstance local;
    private InternalSerializationService serializationService;
    private OverloadedConnectionsPlugin plugin;
    private String remoteKey;

    private volatile boolean stop;

    @Before
    public void setup() {
        Hecaton.shutdownAll();

        Config config = new Config()
                .setProperty(OverloadedConnectionsPlugin.PERIOD_SECONDS.getName(), "1")
                .setProperty(OverloadedConnectionsPlugin.SAMPLES.getName(), "10")
                .setProperty(OverloadedConnectionsPlugin.THRESHOLD.getName(), "2")
                .setProperty(ClusterProperty.IO_OUTPUT_THREAD_COUNT.getName(), "1")
                .setProperty(ClusterProperty.SOCKET_BIND_ANY.getName(), "false");

        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        JoinConfig joinConfig = networkConfig.getJoin();
        joinConfig.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig().setEnabled(true);
        tcpIpConfig.addMember("127.0.0.1:5701");
        tcpIpConfig.addMember("127.0.0.1:5702");

        local = Hecaton.newHecatonInstance(config);
        serializationService = getSerializationService(local);
        HecatonInstance remote = Hecaton.newHecatonInstance(config);

        assertClusterSizeEventually(2, local, remote);

        plugin = new OverloadedConnectionsPlugin(getNodeEngineImpl(local));
        plugin.onStart();

        warmUpPartitions(local, remote);
        remoteKey = generateKeyOwnedBy(remote);
    }

    @After
    public void tearDown() {
        stop = true;
        Hecaton.shutdownAll();
    }

    @Test
    public void test() {
        spawn(() -> {
            IMap<String, String> map = local.getMap(getClass().getName());
            while (!stop) {
                map.getAsync(remoteKey);
            }
        });

        assertTrueEventually(() -> {
            plugin.run(logWriter);

            assertContains(GetOperation.class.getSimpleName() + " sampleCount=");
        });
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void toKey() {
        assertToKey(DummyOperation.class.getName(), new DummyOperation());
        assertToKey(Integer.class.getName(), Integer.valueOf(10));
        assertToKey("Backup(" + DummyOperation.class.getName() + ")",
                new Backup(new DummyOperation(), getAddress(local), new long[0], true));
    }

    private void assertToKey(String key, Object object) {
        Packet packet = new Packet(serializationService.toBytes(object));
        assertEquals(key, plugin.toKey(packet));
    }
}
