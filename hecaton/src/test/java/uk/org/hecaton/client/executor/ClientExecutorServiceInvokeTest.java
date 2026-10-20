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

package uk.org.hecaton.client.executor;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.XmlClientConfigBuilder;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.client.test.executor.tasks.AppendCallable;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.test.HecatonTestSupport.randomString;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExecutorServiceInvokeTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();
    private HecatonInstance client;

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void setup()
            throws IOException {
        Config config = new XmlConfigBuilder(getClass().getClassLoader().getResourceAsStream("hecaton-test-executor.xml"))
                .build();
        ClientConfig clientConfig = new XmlClientConfigBuilder("classpath:hecaton-client-test-executor.xml").build();

        hecatonFactory.newHecatonInstance(config);
        client = hecatonFactory.newHecatonClient(clientConfig);
    }

    @Test
    public void testInvokeAll()
            throws Throwable {
        IExecutorService service = client.getExecutorService(randomString());
        String msg = randomString();
        Collection<Callable<String>> collection = new ArrayList<>();
        collection.add(new AppendCallable(msg));
        collection.add(new AppendCallable(msg));

        List<Future<String>> results = service.invokeAll(collection);
        for (Future<String> result : results) {
            assertEquals(msg + AppendCallable.APPENDAGE, result.get());
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAll_withTimeOut()
            throws Throwable {
        IExecutorService service = client.getExecutorService(randomString());
        Collection<Callable<String>> collection = new ArrayList<>();
        collection.add(new AppendCallable());
        collection.add(new AppendCallable());

        service.invokeAll(collection, 1, TimeUnit.MINUTES);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAny()
            throws Throwable {
        IExecutorService service = client.getExecutorService(randomString());
        Collection<Callable<String>> collection = new ArrayList<>();
        collection.add(new AppendCallable());
        collection.add(new AppendCallable());

        service.invokeAny(collection);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAnyTimeOut()
            throws Throwable {
        IExecutorService service = client.getExecutorService(randomString());
        Collection<Callable<String>> collection = new ArrayList<>();
        collection.add(new AppendCallable());
        collection.add(new AppendCallable());
        service.invokeAny(collection, 1, TimeUnit.MINUTES);
    }
}
