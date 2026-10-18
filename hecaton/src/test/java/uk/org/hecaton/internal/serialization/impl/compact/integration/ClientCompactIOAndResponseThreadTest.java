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

package uk.org.hecaton.internal.serialization.impl.compact.integration;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.lang.String.valueOf;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for a deadlock on either IO thread or reponse thread.
 * A response message is processed either on IO thread or response thread
 * depending on which response handler is used:
 * - SyncResponseHandler always uses the IO thread to process the message
 * - AsyncResponseHandler always uses the response thread to process the message
 * - DynamicResponseHandler uses concurrency detection to decide if it should\
 * offload the message processing to a response thread or use the IO thread,
 * so it is identical to one of the cases above, hence there is no test case
 * for it.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category({SlowTest.class})
public class ClientCompactIOAndResponseThreadTest extends HecatonTestSupport {

    private static final ILogger logger = Logger.getLogger(ClientCompactIOAndResponseThreadTest.class);

    protected TestHecatonFactory factory = new TestHecatonFactory();

    // This is the default value for heartbeat timeout, setting it for clarity
    private static final int HEARTBEAT_TIMEOUT = 60_000;

    // Timeout for the tests in this class, you can lower this value when debugging locally
    // This must be lower than heartbeat timeout
    private static final int TEST_TIMEOUT = HEARTBEAT_TIMEOUT / 2;

    private HecatonInstance member;

    record SimpleRecord(String name) {
    }

    @Before
    public void setUp() throws Exception {
        member = factory.newHecatonInstance();
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    // ISSUE 1 - sync handler - response always processed on the IO thread
    @Test(timeout = TEST_TIMEOUT)
    public void getCompactValueUsingSyncResponseHandler() {
        HecatonInstance client1 = factory.newHecatonClient(new ClientConfig());
        IMap<Integer, SimpleRecord> myMap1 = client1.getMap("my-map");
        // Set value using client 1 - using zero compact serialization
        SimpleRecord record1 = new SimpleRecord("test");
        myMap1.put(1, record1);
        client1.shutdown();

        logger.info("value set to " + record1);

        ClientConfig config2 = new ClientConfig();
        // Due to this property the SyncResponseHandler in ClientResponseHandlerSupplier is used
        config2.setProperty("hecaton.client.response.thread.count", "0");
        config2.setProperty("hecaton.client.heartbeat.timeout", valueOf(HEARTBEAT_TIMEOUT));
        HecatonInstance client2 = factory.newHecatonClient(config2);
        IMap<Integer, SimpleRecord> myMap2 = client2.getMap("my-map");
        SimpleRecord record2 = myMap2.getAsync(1).toCompletableFuture().join();
        assertThat(record2).isEqualTo(record1);
    }

    // ISSUE 2 - async response handler - get data and fetch schema responses on the same thread
    // Removing test timeout shows that this blocks indefinitely
    @Test(timeout = TEST_TIMEOUT)
    public void getCompactValueUsingAsyncResponseHandler() throws Exception {
        HecatonInstance client1 = factory.newHecatonClient(new ClientConfig());
        IMap<Integer, SimpleRecord> myMap1 = client1.getMap("my-map");
        // Set value using client 1 - using zero compact serialization
        SimpleRecord record1 = new SimpleRecord("test");
        myMap1.put(1, record1);
        client1.shutdown();

        logger.info("Wrote entry " + record1);

        ClientConfig config2 = new ClientConfig();
        // use only single response thread - it ensures that we hit the issue with same response
        // thread processing the two responses
        config2.setProperty("hecaton.client.response.thread.count", "1");
        config2.setProperty("hecaton.client.response.thread.dynamic", "false");

        config2.setProperty("hecaton.client.heartbeat.timeout", valueOf(HEARTBEAT_TIMEOUT));
        HecatonInstance client2 = factory.newHecatonClient(config2);
        IMap<Integer, SimpleRecord> myMap2 = client2.getMap("my-map");
        SimpleRecord record2 = myMap2.getAsync(1).toCompletableFuture().join();
        assertThat(record2).isEqualTo(record1);
    }

}
