/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.client.cp.internal.datastructures.cpmap;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPMap;
import uk.org.hecaton.cp.internal.datastructures.cpmap.AbstractCPMapBasicTest;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPMapClientBasicTest extends AbstractCPMapBasicTest {

    private HecatonInstance client;

    @Override
    protected TestHecatonInstanceFactory createTestFactory() {
        return new TestHecatonFactory();
    }

    @Override
    protected HecatonInstance[] createInstances() {
        HecatonInstance[] members = newInstances(3, 3, 1);
        client = ((TestHecatonFactory) factory).newHecatonClient();
        return members;
    }

    @Override
    protected <K, V> CPMap<K, V> createMap(String name) {
        return client.getCPSubsystem().getMap(name);
    }

    @After
    public void shutdown() {
        factory.terminateAll();
    }

    @Test
    public void testPurgeIsNotAvailable() throws InterruptedException {
        try {
            client.getCPSubsystem().getCPDataStructureManagementService()
                  .purgeCPMap(PURGEABLE_MAP_PREFIX + "Map", Duration.ZERO).toCompletableFuture().get();
            throw new AssertionError("purge succeeded");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof UnsupportedOperationException);
        }
    }
}
