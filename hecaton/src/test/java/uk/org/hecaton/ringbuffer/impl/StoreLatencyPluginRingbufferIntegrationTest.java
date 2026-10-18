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

package uk.org.hecaton.ringbuffer.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.config.RingbufferStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.ringbuffer.RingbufferStore;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;
import java.util.Random;

import static uk.org.hecaton.internal.nio.IOUtil.deleteQuietly;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StoreLatencyPluginRingbufferIntegrationTest extends HecatonTestSupport {

    private HecatonInstance hz;
    private Ringbuffer<Object> rb;

    @Before
    public void setup() throws Exception {
        Config config = new Config()
                .setProperty("hecaton.diagnostics.enabled", "true")
                .setProperty("hecaton.diagnostics.storeLatency.period.seconds", "1");

        RingbufferConfig rbConfig = addRingbufferConfig(config);

        hz = createHecatonInstance(config);
        rb = hz.getRingbuffer(rbConfig.getName());
    }

    @After
    public void after() {
        File file = getNodeEngineImpl(hz).getDiagnostics().currentFile();
        deleteQuietly(file);
    }

    @Test
    public void test() throws Exception {
        for (long k = 0; k <= rb.tailSequence(); k++) {
            rb.readOne(k);
        }

        assertTrueEventually(() -> {
            File file = getNodeEngineImpl(hz).getDiagnostics().currentFile();
            String content = Files.readString(file.toPath());
            assertContains(content, "ringworm");
        });
    }

    private static RingbufferConfig addRingbufferConfig(Config config) {
        RingbufferStore store = new RingbufferStore() {
            private final Random random = new Random();

            @Override
            public void store(long sequence, Object data) {
            }

            @Override
            public void storeAll(long firstItemSequence, Object[] items) {
            }

            @Override
            public Object load(long sequence) {
                randomSleep();
                return sequence;
            }

            @Override
            public long getLargestSequence() {
                randomSleep();
                return 100;
            }

            private void randomSleep() {
                long delay = 1 + random.nextInt(100);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        return config.getRingbufferConfig("ringworm")
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setRingbufferStoreConfig(new RingbufferStoreConfig().setStoreImplementation(store));
    }
}
