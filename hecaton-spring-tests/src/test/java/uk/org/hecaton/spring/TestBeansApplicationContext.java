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

package uk.org.hecaton.spring;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.QueueConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonTestSupport;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static uk.org.hecaton.config.EvictionPolicy.RANDOM;
import static uk.org.hecaton.config.MaxSizePolicy.PER_PARTITION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"beans-applicationContext-hecaton.xml"})
class TestBeansApplicationContext extends HecatonTestSupport {

    public static final String INTERNAL_JET_OBJECTS_PREFIX = "__jet.";

    @BeforeAll
    @AfterAll
    public static void cleanup() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Autowired
    private ApplicationContext context;

    @Test
    void testApplicationContext() {
        assertTrue(HecatonClient.getAllHecatonClients().isEmpty());

        context.getBean("map2");

        assertEquals(1, Hecaton.getAllHecatonInstances().size());
        assertEquals(1, HecatonClient.getAllHecatonClients().size());

        HecatonInstance hecaton = Hecaton.getAllHecatonInstances().iterator().next();
        long distributedObjectsSize = hecaton.getDistributedObjects().stream().filter(
                distributedObject -> !distributedObject.getName().startsWith(INTERNAL_JET_OBJECTS_PREFIX)).count();
        assertEquals(2, distributedObjectsSize);

        context.getBean("client");
        context.getBean("client");
        assertEquals(3, HecatonClient.getAllHecatonClients().size());
        HecatonClientProxy client = (HecatonClientProxy) HecatonClient.getAllHecatonClients().iterator().next();
        assertNull(client.getClientConfig().getManagedContext());

        int batchSize = client.getClientConfig().getQueryCacheConfigs().get("").get("cache1").getBatchSize();
        assertEquals(12, batchSize);

        assertFalse(client.getClientConfig().getMetricsConfig().isEnabled());

        HecatonInstance instance = (HecatonInstance) context.getBean("instance");
        assertEquals(1, Hecaton.getAllHecatonInstances().size());
        assertEquals(instance, Hecaton.getAllHecatonInstances().iterator().next());
        assertNull(instance.getConfig().getManagedContext());
    }

    @Test
    void testPlaceholderInEviction() {
        assertTrue(HecatonClient.getAllHecatonClients().isEmpty());

        assertEquals(1, Hecaton.getAllHecatonInstances().size());

        HecatonInstance instance = (HecatonInstance) context.getBean("instance");
        MapConfig mapConfig = instance.getConfig().getMapConfig("map-with-eviction");

        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        assertThat(evictionConfig.getSize()).isEqualTo(1983);
        assertThat(evictionConfig.getMaxSizePolicy()).isEqualTo(PER_PARTITION);
        assertThat(evictionConfig.getEvictionPolicy()).isEqualTo(RANDOM);
    }

    @Test
    void testPlaceHolder() {
        HecatonInstance instance = (HecatonInstance) context.getBean("instance");
        waitInstanceForSafeState(instance);
        Config config = instance.getConfig();
        assertEquals("spring-cluster", config.getClusterName());
        assertFalse(config.getNetworkConfig().getJoin().getTcpIpConfig().isEnabled());
        assertEquals(6, config.getMapConfig("map1").getBackupCount());
        assertFalse(config.getMapConfig("map1").isStatisticsEnabled());
        assertEquals(64, config.getNativeMemoryConfig().getSize().getValue());
        QueueConfig testQueue = config.getQueueConfig("testQueue");
        assertEquals("uk.org.hecaton.collection.impl.queue.model.PriorityElementComparator",
                testQueue.getPriorityComparatorClassName());
    }
}
