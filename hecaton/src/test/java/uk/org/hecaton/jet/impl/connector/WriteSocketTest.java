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

package uk.org.hecaton.jet.impl.connector;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.Sources;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.jet.core.processor.SinkProcessors.writeSocketP;
import static uk.org.hecaton.jet.core.test.TestSupport.verifyProcessor;
import static uk.org.hecaton.jet.impl.util.Util.uncheckRun;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteSocketTest extends JetTestSupport {

    private static final int ITEM_COUNT = 1000;

    @Test
    public void unitTest() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        ServerSocket serverSocket = new ServerSocket(0);
        Runnable acceptConnection = () -> spawn(() -> uncheckRun(() -> {
            Socket socket = serverSocket.accept();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                while (reader.readLine() != null) {
                    counter.incrementAndGet();
                }
            }
        }));

        verifyProcessor(writeSocketP("localhost", serverSocket.getLocalPort(), Object::toString, UTF_8))
                .input(range(0, ITEM_COUNT).boxed().collect(toList()))
                .executeBeforeEachRun(acceptConnection)
                .disableSnapshots()
                .expectOutput(emptyList());

        int expectedReceivedItems = ITEM_COUNT * 2; // TestSupport executed each test twice
        assertEqualsEventually(expectedReceivedItems, counter);
        // wait a little to check, if the counter doesn't get too far
        Thread.sleep(500);
        assertEquals(expectedReceivedItems, counter.get());

        serverSocket.close();
    }

    @Test
    public void integrationTest() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        ServerSocket serverSocket = new ServerSocket(0);
        spawn(() -> uncheckRun(() -> {
            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                spawn(() -> uncheckRun(() -> {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                        while (reader.readLine() != null) {
                            counter.incrementAndGet();
                        }
                    }
                }));
            }
        }));

        HecatonInstance hz = createHecatonInstance();
        createHecatonInstance();
        IMap<Integer, String> map = hz.getMap("map");
        range(0, ITEM_COUNT).forEach(i -> map.put(i, String.valueOf(i)));

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map("map"))
         .writeTo(Sinks.socket("localhost", serverSocket.getLocalPort()));

        hz.getJet().newJob(p).join();
        assertTrueEventually(() -> assertEquals(ITEM_COUNT, counter.get()));
        serverSocket.close();
        // wait a little to check, if the counter doesn't get too far
        Thread.sleep(500);
        assertEquals(ITEM_COUNT, counter.get());
    }

}
