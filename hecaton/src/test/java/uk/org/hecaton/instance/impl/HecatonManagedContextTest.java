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

package uk.org.hecaton.instance.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.core.ManagedContext;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.SerializationServiceAware;
import uk.org.hecaton.internal.services.NodeAware;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonManagedContextTest extends HecatonTestSupport {

    private DependencyInjectionUserClass userClass;
    private DependencyInjectionUserManagedContext userContext;
    private HecatonInstance hecatonInstance;
    private Node node;
    private SerializationService serializationService;

    @Before
    public void setUp() {
        userClass = new DependencyInjectionUserClass();
        userContext = new DependencyInjectionUserManagedContext();

        Config config = getConfig()
                .setManagedContext(userContext);

        hecatonInstance = createHecatonInstance(config);
        node = getNode(hecatonInstance);
        serializationService = getSerializationService(hecatonInstance);
    }

    @Test
    public void testInitialize() {
        DependencyInjectionUserClass initializedUserClass = (DependencyInjectionUserClass) serializationService.getManagedContext().initialize(userClass);

        assertEquals(hecatonInstance, initializedUserClass.hecatonInstance);
        assertEquals(node, initializedUserClass.node);
        assertEquals(serializationService, initializedUserClass.serializationService);
        assertTrue(userContext.wasCalled);
    }

    private static class DependencyInjectionUserClass implements HecatonInstanceAware, NodeAware, SerializationServiceAware {

        private HecatonInstance hecatonInstance;
        private Node node;
        private SerializationService serializationService;

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            this.hecatonInstance = hecatonInstance;
        }

        @Override
        public void setNode(Node node) {
            this.node = node;
        }

        @Override
        public void setSerializationService(SerializationService serializationService) {
            this.serializationService = serializationService;
        }
    }

    private static class DependencyInjectionUserManagedContext implements ManagedContext {

        private boolean wasCalled;

        @Override
        public Object initialize(Object obj) {
            wasCalled = true;
            return obj;
        }
    }
}
