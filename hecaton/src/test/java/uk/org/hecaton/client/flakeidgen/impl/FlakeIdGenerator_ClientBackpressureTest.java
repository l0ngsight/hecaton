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

package uk.org.hecaton.client.flakeidgen.impl;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientFlakeIdGeneratorConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.flakeidgen.impl.FlakeIdGenerator_AbstractBackpressureTest;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class FlakeIdGenerator_ClientBackpressureTest extends FlakeIdGenerator_AbstractBackpressureTest {

    private TestHecatonFactory factory;

    @Before
    public void before() {
        factory = new TestHecatonFactory(1);
        factory.newHecatonInstance();
        instance = factory.newHecatonClient(new ClientConfig().addFlakeIdGeneratorConfig(new ClientFlakeIdGeneratorConfig("gen")
                .setPrefetchCount(BATCH_SIZE)));
    }

    @After
    public void after() {
        factory.shutdownAll();
    }
}
