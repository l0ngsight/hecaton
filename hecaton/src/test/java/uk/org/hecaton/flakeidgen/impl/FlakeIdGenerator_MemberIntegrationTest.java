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

package uk.org.hecaton.flakeidgen.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.flakeidgen.FlakeIdGenerator;
import uk.org.hecaton.internal.monitor.LocalFlakeIdGeneratorStats;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.annotation.SerializationSamplesExcluded;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class, SerializationSamplesExcluded.class})
public class FlakeIdGenerator_MemberIntegrationTest extends HecatonTestSupport {

    private TestHecatonInstanceFactory factory;

    @Before
    public void before() {
        factory = createHecatonInstanceFactory();
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void smokeTest() throws Exception {
        HecatonInstance instance = factory.newHecatonInstance();
        final FlakeIdGenerator generator = instance.getFlakeIdGenerator("gen");
        FlakeIdConcurrencyTestUtil.concurrentlyGenerateIds(generator::newId);
    }

    @Test
    public void statistics() {
        HecatonInstance instance = factory.newHecatonInstance();

        FlakeIdGenerator gen = instance.getFlakeIdGenerator("gen");
        gen.newId();

        FlakeIdGeneratorService service = getNodeEngineImpl(instance).getService(FlakeIdGeneratorService.SERVICE_NAME);
        Map<String, LocalFlakeIdGeneratorStats> stats = service.getStats();
        assertFalse(stats.isEmpty());
        assertTrue(stats.containsKey("gen"));
        LocalFlakeIdGeneratorStats genStats = stats.get("gen");
        assertEquals(1L, genStats.getBatchCount());
        assertTrue(genStats.getIdCount() > 0);
    }
}
