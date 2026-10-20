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

package uk.org.hecaton.internal.serialization.impl.compact.schema;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonNodeExtension;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.serialization.impl.compact.Schema;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import example.serialization.NodeDTO;
import org.junit.After;
import org.junit.Before;

import java.util.function.IntFunction;

import static uk.org.hecaton.instance.impl.TestUtil.getNode;
import static uk.org.hecaton.internal.serialization.impl.compact.CompactTestUtil.getSchemasFor;
import static org.mockito.Mockito.spy;

public class CompactSchemaReplicationTestBase extends HecatonTestSupport {

    private static final String MAP_NAME = "map";

    protected static final Schema SCHEMA = getSchemasFor(NodeDTO.class).iterator().next();

    protected HecatonInstance[] instances;

    private TestHecatonInstanceFactory factory;

    protected MemberSchemaService stubbedSchemaService;

    @Before
    public void setUp() {
        factory = new TestHecatonInstanceFactory();
        stubbedSchemaService = createSpiedMemberSchemaService();
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }

    public void setupInstances(IntFunction<MemberSchemaService> spyServiceCreator) {
        int instanceCount = 4;
        instances = new HecatonInstance[instanceCount];
        for (int i = 0; i < 4; i++) {
            MemberSchemaService schemaService = spyServiceCreator.apply(i);
            factory.withNodeExtensionCustomizer(node -> new MemberSchemaServiceMockingNodeExtension(node, schemaService));
            instances[i] = factory.newHecatonInstance(getConfig());
        }
    }

    @Override
    public Config getConfig() {
        return smallInstanceConfigWithoutJetAndMetrics();
    }

    protected void fillMapUsing(HecatonInstance instance) {
        IMap<Integer, NodeDTO> map = instance.getMap(MAP_NAME);
        for (int i = 0; i < 10; i++) {
            map.put(i, new NodeDTO(i));
        }
    }

    protected MemberSchemaService getSchemaService(HecatonInstance instance) {
        return getNode(instance).getSchemaService();
    }

    protected MemberSchemaService createSpiedMemberSchemaService() {
        return spy(new MemberSchemaService());
    }

    private static class MemberSchemaServiceMockingNodeExtension extends HecatonNodeExtension {
        private final MemberSchemaService schemaService;

        MemberSchemaServiceMockingNodeExtension(Node node, MemberSchemaService schemaService) {
            super(node);
            this.schemaService = schemaService;
        }

        @Override
        public MemberSchemaService createSchemaService() {
            return schemaService;
        }
    }
}
