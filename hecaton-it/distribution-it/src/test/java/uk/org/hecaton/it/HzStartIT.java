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

package uk.org.hecaton.it;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class HzStartIT extends HecatonTestSupport {

    private static final ILogger log = Logger.getLogger(HzStartIT.class);

    private Process process;
    private HecatonInstance client;
    private String clusterName;

    @Before
    public void setUp() throws Exception {
        assumeThatNoWindowsOS();

        // Use custom cluster name to isolate concurrently running tests
        clusterName = this.getClass().getSimpleName() + "_" + UUID.randomUUID();

        ProcessBuilder builder = new ProcessBuilder("bin/hc", "start")
                .directory(new File("./target/hecaton"))
                .inheritIO();
        builder.environment().put("HC_CLUSTERNAME", clusterName);
        // Remove classpath set by Maven
        builder.environment().remove("CLASSPATH");

        process = builder.start();
        client = HecatonClient.newHecatonClient(new ClientConfig().setClusterName(clusterName));
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.shutdown();
        }
        if (process != null) {
            log.info("Destroying Hecaton process");
            process.destroy();
            boolean destroyed = process.waitFor(30, TimeUnit.SECONDS);
            if (!destroyed) {
                log.info("Hecaton process not destroyed, trying Process#destroyForcibly()");
                process.destroyForcibly()
                       .waitFor();
            }
        }
    }

    @Test
    public void shouldStartHecatonWithHzStart() {
        IMap<String, Integer> map = client.getMap("map");
        map.put("key", 42);
        assertThat(map.get("key")).isEqualTo(42);
    }
}
