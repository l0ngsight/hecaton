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

package uk.org.hecaton.client.console;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.OverridePropertyRule;
import uk.org.hecaton.test.TestAwareInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static uk.org.hecaton.internal.util.StringUtil.stringToBytes;
import static uk.org.hecaton.test.AbstractHecatonClassRunner.getTestMethodName;
import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static uk.org.hecaton.test.HecatonTestSupport.sleepSeconds;
import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end test(s) for {@link ClientConsoleApp}. The tests use real network.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category({ QuickTest.class })
public class ClientConsoleTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public OverridePropertyRule propertyClientConfig = OverridePropertyRule.clear("hecaton.client.config");

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @After
    public void after() {
        factory.terminateAll();
    }

    /**
     * Tests if the application correctly connects to a Hecaton cluster. The TCP discovery is configured by using a custom
     * YAML client configuration file.
     */
    @Test
    public void connectsToHecatonCluster() throws IOException {
        HecatonInstance hz = factory.newHecatonInstance(smallInstanceConfig());
        Address address = getAddress(hz);
        File cfgFile = tempFolder.newFile("hecaton-config.yml");
        try (BufferedWriter writer = Files.newBufferedWriter(cfgFile.toPath())) {
            writer.write("hecaton-client:\n"
                    + "  cluster-name: " + getTestMethodName() + "\n"
                    + "  instance-name: clientConsoleApp\n"
                    + "  network:\n"
                    + "    cluster-members:\n"
                    + "      - " + address.getHost() + ":" + address.getPort() + "\n");
        }
        propertyClientConfig.setOrClearProperty(cfgFile.getAbsolutePath());
        assertTrue(hz.getClientService().getConnectedClients().isEmpty());

        ExecutorService tp = Executors.newFixedThreadPool(1, r -> new Thread(r, "consoleAppTestThread"));
        InputStream origIn = System.in;
        try {
            LineEndingsInputStream bqIn = new LineEndingsInputStream();
            System.setIn(bqIn);
            tp.execute(() -> ClientConsoleApp.run(HecatonClient.newHecatonClient()));

            // Give some time to executor work otherwise,
            // HecatonClient.shutdownAll() can be quicker
            sleepSeconds(5);

            assertTrueEventually(() -> assertFalse(hz.getClientService().getConnectedClients().isEmpty()));
            HecatonInstance client = HecatonClient.getHecatonClientByName("clientConsoleApp");
            assertNotNull(client);
            client.shutdown();
            bqIn.unblock();
        } finally {
            System.setIn(origIn);
            HecatonClient.shutdownAll();
            tp.shutdownNow();
        }
    }

    static class LineEndingsInputStream extends InputStream {

        private static final byte[] NEWLINE_BYTES = stringToBytes(System.lineSeparator());
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile int pos = NEWLINE_BYTES.length - 1;

        @Override
        public int read() throws IOException {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            pos = (pos + 1) % NEWLINE_BYTES.length;
            return NEWLINE_BYTES[pos];
        }

        public void unblock() {
            latch.countDown();
        }
    }
}
