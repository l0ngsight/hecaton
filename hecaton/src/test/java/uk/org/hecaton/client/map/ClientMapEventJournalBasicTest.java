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

package uk.org.hecaton.client.map;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.impl.journal.MapEventJournalBasicTest;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapEventJournalBasicTest extends MapEventJournalBasicTest {

    private TestHecatonFactory factory;
    private HecatonInstance client;

    private final AtomicBoolean isPredicateContextInjected = new AtomicBoolean();
    private final AtomicBoolean isProjectionContextInjected = new AtomicBoolean();

    @Override
    protected HecatonInstance getRandomInstance() {
        return client;
    }

    @Override
    protected HecatonInstance[] createInstances() {
        factory = new TestHecatonFactory();
        final HecatonInstance[] instances = factory.newInstances(getConfig(), 2);
        client = factory.newHecatonClient();
        return instances;
    }

    @Override
    public void setPredicate(boolean value) {
        this.isPredicateContextInjected.set(value);
    }

    @Override
    public void setProjection(boolean value) {
        this.isProjectionContextInjected.set(value);
    }
    @Override
    public AtomicBoolean getPredicate() {
        return this.isPredicateContextInjected;
    }

    @Override
    public AtomicBoolean getProjection() {
        return this.isProjectionContextInjected;
    }


    @After
    public final void terminate() {
        factory.terminateAll();
    }
}
