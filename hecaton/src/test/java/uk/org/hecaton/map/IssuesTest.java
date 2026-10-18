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

package uk.org.hecaton.map;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.GlobalSerializerConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.StreamSerializer;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IssuesTest extends HecatonTestSupport {

    @Test
    public void testIssue321_1() throws Exception {
        int n = 1;
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(n);

        final IMap<Integer, Integer> map = factory.newHecatonInstance(getConfig()).getMap("testIssue321_1");
        final BlockingQueue<uk.org.hecaton.core.EntryEvent<Integer, Integer>> events1
                = new LinkedBlockingQueue<>();
        final BlockingQueue<uk.org.hecaton.core.EntryEvent<Integer, Integer>> events2
                = new LinkedBlockingQueue<>();
        map.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(uk.org.hecaton.core.EntryEvent<Integer, Integer> event) {
                events2.add(event);
            }
        }, false);
        map.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(uk.org.hecaton.core.EntryEvent<Integer, Integer> event) {
                events1.add(event);
            }
        }, true);
        map.put(1, 1);
        final uk.org.hecaton.core.EntryEvent<Integer, Integer> event1 = events1.poll(10, TimeUnit.SECONDS);
        final uk.org.hecaton.core.EntryEvent<Integer, Integer> event2 = events2.poll(10, TimeUnit.SECONDS);
        assertNotNull(event1);
        assertNotNull(event2);
        assertNotNull(event1.getValue());
        assertNull(event2.getValue());
    }

    @Test
    public void testIssue321_2() throws Exception {
        int n = 1;
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(n);

        final IMap<Integer, Integer> imap = factory.newHecatonInstance(getConfig()).getMap("testIssue321_2");
        final BlockingQueue<uk.org.hecaton.core.EntryEvent<Integer, Integer>> events1
                = new LinkedBlockingQueue<>();
        final BlockingQueue<uk.org.hecaton.core.EntryEvent<Integer, Integer>> events2
                = new LinkedBlockingQueue<>();
        imap.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(uk.org.hecaton.core.EntryEvent<Integer, Integer> event) {
                events1.add(event);
            }
        }, true);
        Thread.sleep(50L);
        imap.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(uk.org.hecaton.core.EntryEvent<Integer, Integer> event) {
                events2.add(event);
            }
        }, false);
        imap.put(1, 1);
        final uk.org.hecaton.core.EntryEvent<Integer, Integer> event1 = events1.poll(10, TimeUnit.SECONDS);
        final uk.org.hecaton.core.EntryEvent<Integer, Integer> event2 = events2.poll(10, TimeUnit.SECONDS);
        assertNotNull(event1);
        assertNotNull(event2);
        assertNotNull(event1.getValue());
        assertNull(event2.getValue());
    }

    // verify an event including values and an event excluding values are received, in any order
    @Test
    public void testIssue321_3() throws Exception {
        int n = 1;
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(n);

        final IMap<Integer, Integer> imap = factory.newHecatonInstance(getConfig()).getMap("testIssue321_3");
        final List<EntryEvent<Integer, Integer>> eventsWithValues = new ArrayList<>();
        final List<EntryEvent<Integer, Integer>> eventsWithoutValues = new ArrayList<>();
        final EntryAdapter<Integer, Integer> listener = new EntryAdapter<>() {
            @Override
            public void entryAdded(uk.org.hecaton.core.EntryEvent<Integer, Integer> event) {
                if (event.getValue() == null) {
                    eventsWithoutValues.add(event);
                } else {
                    eventsWithValues.add(event);
                }
            }
        };
        imap.addEntryListener(listener, true);
        Thread.sleep(50L);
        imap.addEntryListener(listener, false);
        imap.put(1, 1);
        assertEqualsEventually(eventsWithValues::size, 1);
        assertEqualsEventually(eventsWithoutValues::size, 1);
    }

    @Test
    public void testIssue304() {
        int n = 1;
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(n);
        IMap<String, String> map = factory.newHecatonInstance(getConfig()).getMap("testIssue304");
        map.lock("1");
        assertEquals(0, map.size());
        assertEquals(0, map.entrySet().size());
        map.put("1", "value");
        assertEquals(1, map.size());
        assertEquals(1, map.entrySet().size());
        map.unlock("1");
        assertEquals(1, map.size());
        assertEquals(1, map.entrySet().size());
        map.remove("1");
        assertEquals(0, map.size());
        assertEquals(0, map.entrySet().size());
    }

    @Test
    public void testIssue174NearCacheContainsKeySingleNode() {
        int n = 1;
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(n);
        Config config = getConfig();
        config.setClusterName("testIssue174NearCacheContainsKeySingleNode");
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);
        HecatonInstance h = factory.newHecatonInstance(config);
        IMap<String, String> map = h.getMap("testIssue174NearCacheContainsKeySingleNode");
        map.put("key", "value");
        assertTrue(map.containsKey("key"));
        h.shutdown();
    }

    @Test
    public void testIssue1067GlobalSerializer() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);

        final Config config = getConfig();
        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
        globalSerializerConfig.setOverrideJavaSerialization(false);
        config.getSerializationConfig().setGlobalSerializerConfig(globalSerializerConfig
                .setImplementation(new StreamSerializer() {
                    public void write(ObjectDataOutput out, Object object) {
                    }

                    public Object read(ObjectDataInput in) {
                        return new DummyValue();
                    }

                    public int getTypeId() {
                        return 123;
                    }

                    @Override
                    public void destroy() {
                    }
                }));

        HecatonInstance hz = factory.newHecatonInstance(config);
        IMap<Object, Object> map = hz.getMap("test");
        for (int i = 0; i < 10; i++) {
            map.put(i, new DummyValue());
        }
        assertEquals(10, map.size());

        HecatonInstance hz2 = factory.newHecatonInstance(config);
        IMap<Object, Object> map2 = hz2.getMap("test");
        assertEquals(10, map2.size());
        assertEquals(10, map.size());

        for (int i = 0; i < 10; i++) {
            Object o = map2.get(i);
            assertNotNull(o);
            assertTrue(o instanceof DummyValue);
        }
    }

    private static class DummyValue {
    }

    @Test
    public void testMapInterceptorInstanceAware() {
        Config config = getConfig();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance hz1 = factory.newHecatonInstance(config);
        HecatonInstance hz2 = factory.newHecatonInstance(config);
        IMap<Object, Object> map = hz1.getMap("test");

        InstanceAwareMapInterceptorImpl interceptor = new InstanceAwareMapInterceptorImpl();
        map.addInterceptor(interceptor);
        assertNotNull(interceptor.hecatonInstance);
        assertEquals(hz1, interceptor.hecatonInstance);

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < 100; i++) {
            assertEquals("notNull", map.get(i));
        }
    }

    static class InstanceAwareMapInterceptorImpl extends MapInterceptorAdaptor implements HecatonInstanceAware {
        @Serial
        private static final long serialVersionUID = 1L;

        transient HecatonInstance hecatonInstance;

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            this.hecatonInstance = hecatonInstance;
        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            if (hecatonInstance != null) {
                return "notNull";
            }
            return ">null";
        }
    }

    @Test // Issue #1795
    public void testMapClearDoesNotTriggerEqualsOrHashCodeOnKeyObject() {
        int n = 1;
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(n);
        final HecatonInstance instance = factory.newHecatonInstance(getConfig());
        final IMap<CompositeKey, String> map = instance.getMap(randomString());
        final CompositeKey key = new CompositeKey();
        map.put(key, "value");
        map.clear();
        assertFalse("hashCode method should not have been called on key during clear", CompositeKey.hashCodeCalled);
        assertFalse("equals method should not have been called on key during clear", CompositeKey.equalsCalled);
    }

    public static class CompositeKey implements Serializable {

        static boolean hashCodeCalled = false;
        static boolean equalsCalled = false;

        @Override
        public int hashCode() {
            hashCodeCalled = true;
            return super.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            equalsCalled = true;
            return super.equals(o);
        }
    }
}
