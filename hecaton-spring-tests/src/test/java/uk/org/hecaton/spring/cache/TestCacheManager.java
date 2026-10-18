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

package uk.org.hecaton.spring.cache;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.DistributedObjectEvent;
import uk.org.hecaton.core.DistributedObjectListener;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spring.CustomSpringExtension;
import uk.org.hecaton.test.HecatonTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.test.HecatonTestSupport.assertClusterSizeEventually;
import static uk.org.hecaton.test.HecatonTestSupport.assertContains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;


@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"cacheManager-applicationContext-hecaton.xml"})
class TestCacheManager {

    @Autowired
    private HecatonInstance instance;

    @Autowired
    private IDummyBean bean;

    @Autowired
    private CacheManager cacheManager;

    @BeforeAll
    @AfterAll
    public static void start() {
        Hecaton.shutdownAll();
    }

    @Test
    void testBean_withValue() {
        for (int i = 0; i < 100; i++) {
            assertEquals("name:" + i, bean.getName(i));
            assertEquals("city:" + i, bean.getCity(i));
        }
    }

    @Test
    void testBean_withNull() {
        for (int i = 0; i < 100; i++) {
            assertNull(bean.getNull());
        }
    }

    @Test
    void testBean_withTTL() {
        String name = bean.getNameWithTTL();
        assertEquals("ali", name);
        String nameFromCache = bean.getNameWithTTL();
        assertEquals("ali", nameFromCache);

        HecatonTestSupport.sleepSeconds(3);

        String nameFromCacheAfterTTL = bean.getNameWithTTL();
        assertNull(nameFromCacheAfterTTL);
    }

    @Test
    void testCacheNames() {
        // create a test instance, to reproduce the behavior described in the GitHub issue
        // https://github.com/hazelcast/hazelcast/issues/492
        final String testMap = "test-map";

        final CountDownLatch distributionSignal = new CountDownLatch(1);
        instance.addDistributedObjectListener(new DistributedObjectListener() {
            @Override
            public void distributedObjectCreated(DistributedObjectEvent event) {
                DistributedObject distributedObject = event.getDistributedObject();
                if (distributedObject instanceof IMap) {
                    IMap<?, ?> map = (IMap) distributedObject;
                    if (testMap.equals(map.getName())) {
                        distributionSignal.countDown();
                    }
                }
            }

            @Override
            public void distributedObjectDestroyed(DistributedObjectEvent event) {
            }
        });

        HecatonInstance testInstance = Hecaton.newHecatonInstance(extractConfig());
        testInstance.getMap(testMap);
        // be sure that test-map is distributed
        HecatonTestSupport.assertOpenEventually(distributionSignal);

        Collection<String> test = cacheManager.getCacheNames();
        assertContains(test, testMap);
        testInstance.shutdown();
        // Wait for the cluster to scale down, so it doesn't affect other tests
        assertClusterSizeEventually(1, instance);
    }

    public static class DummyBean implements IDummyBean {

        final AtomicBoolean nullCall = new AtomicBoolean();
        final AtomicBoolean firstCall = new AtomicBoolean();

        @Override
        public String getName(int k) {
            fail("should not call this method!");
            return null;
        }

        @Override
        public String getCity(int k) {
            fail("should not call this method!");
            return null;
        }

        @Override
        public Object getNull() {
            if (nullCall.compareAndSet(false, true)) {
                return null;
            }
            fail("should not call this method!");
            return null;
        }

        @Override
        public String getNameWithTTL() {
            if (firstCall.compareAndSet(false, true)) {
                return "ali";
            }
            return null;
        }
    }

    private Config extractConfig() {
        Config config = instance.getConfig();
        Config extractedConfig = new Config();
        extractedConfig
                .setProperties(config.getProperties())
                .setClusterName(config.getClusterName())
                .setNetworkConfig(config.getNetworkConfig())
                .setJetConfig(config.getJetConfig())
                .setSqlConfig(config.getSqlConfig());

        return extractedConfig;
    }
}
