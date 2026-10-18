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

package uk.org.hecaton.spring.replicatedmap;

import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.ReplicatedMapConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spring.CustomSpringExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"replicatedMap-applicationContext-hecaton.xml"})
class TestReplicatedMapApplicationContext {

    @Autowired
    private HecatonInstance instance;

    @BeforeAll
    @AfterAll
    static void start() {
        Hecaton.shutdownAll();
    }

    @Test
    void testReplicatedMapConfig() {
        ReplicatedMapConfig replicatedMapConfig = instance.getConfig().getReplicatedMapConfig("replicatedMap");
        assertNotNull(replicatedMapConfig);
        assertEquals("OBJECT", InMemoryFormat.OBJECT.name());
        assertTrue(replicatedMapConfig.isAsyncFillup());
        assertFalse(replicatedMapConfig.isStatisticsEnabled());

        List<ListenerConfig> listenerConfigs = replicatedMapConfig.getListenerConfigs();
        assertEquals(1, listenerConfigs.size());
        assertEquals("uk.org.hecaton.spring.DummyEntryListener", listenerConfigs.get(0).getClassName());
    }
}
