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

package uk.org.hecaton.client.standalone;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.standalone.model.MyElement;
import uk.org.hecaton.client.standalone.model.MyKey;
import uk.org.hecaton.client.standalone.model.MyPortableElement;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.internal.util.FilteringClassLoader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.query.Predicates.equal;
import static uk.org.hecaton.query.Predicates.in;
import static uk.org.hecaton.query.Predicates.or;
import static uk.org.hecaton.test.HecatonTestSupport.assertOpenEventually;
import static uk.org.hecaton.test.HecatonTestSupport.randomString;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMapStandaloneTest {

    private static final ClassLoader FILTERING_CLASS_LOADER;

    private static HecatonInstance client;

    static {
        List<String> excludes = singletonList("uk.org.hecaton.client.standalone.model");
        FILTERING_CLASS_LOADER = new FilteringClassLoader(excludes, "uk.org.hecaton");
    }

    @BeforeClass
    public static void init() throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        thread.setContextClassLoader(FILTERING_CLASS_LOADER);

        try {
            Class<?> configClazz = FILTERING_CLASS_LOADER.loadClass("uk.org.hecaton.config.Config");
            Object config = configClazz.newInstance();
            Method setClassLoader = configClazz.getDeclaredMethod("setClassLoader", ClassLoader.class);

            setClassLoader.invoke(config, FILTERING_CLASS_LOADER);

            Class<?> hecatonClazz = FILTERING_CLASS_LOADER.loadClass("uk.org.hecaton.core.Hecaton");
            Method newHecatonInstance = hecatonClazz.getDeclaredMethod("newHecatonInstance", configClazz);

            newHecatonInstance.invoke(hecatonClazz, config);
        } finally {
            thread.setContextClassLoader(tccl);
        }
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addPortableFactory(MyPortableElement.FACTORY_ID, new MyPortableElement.Factory());
        client = HecatonClient.newHecatonClient(clientConfig);
    }

    @AfterClass
    public static void destroy() throws Exception {
        client.shutdown();

        Class<?> hecatonClazz = FILTERING_CLASS_LOADER.loadClass("uk.org.hecaton.core.Hecaton");
        Method shutdownAll = hecatonClazz.getDeclaredMethod("shutdownAll");
        shutdownAll.invoke(hecatonClazz);
    }

    @Test
    public void testPut() {
        MyKey key = new MyKey();
        MyElement element = new MyElement(randomString());

        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        thread.setContextClassLoader(FILTERING_CLASS_LOADER);

        try {
            IMap<MyKey, MyElement> map = createMap();
            map.put(key, element);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    @Test
    public void testGet() {
        IMap<MyKey, MyElement> map = createMap();

        MyKey key = new MyKey();
        MyElement element = new MyElement(randomString());

        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        thread.setContextClassLoader(FILTERING_CLASS_LOADER);

        try {
            map.put(key, element);
            MyElement result = map.get(key);
            assertEquals(element, result);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    @Test
    public void testRemove() {
        IMap<MyKey, MyElement> map = createMap();

        MyKey key = new MyKey();
        MyElement element = new MyElement(randomString());

        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        thread.setContextClassLoader(FILTERING_CLASS_LOADER);

        try {
            map.put(key, element);
            MyElement result = map.remove(key);
            assertEquals(element, result);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    @Test
    public void testClear() {
        IMap<MyKey, MyElement> map = createMap();

        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        thread.setContextClassLoader(FILTERING_CLASS_LOADER);

        try {
            MyKey key = new MyKey();
            MyElement element = new MyElement(randomString());
            map.put(key, element);
            map.clear();
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    @Test
    public void testPortable_withEntryListenerWithPredicate() {
        int key = 1;
        int id = 1;

        IMap<Integer, MyPortableElement> map = createMap();
        Predicate<Integer, MyPortableElement> predicate = equal("id", id);
        MyPortableElement element = new MyPortableElement(id);
        final CountDownLatch eventLatch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<Integer, MyPortableElement>() {
            @Override
            public void onEntryEvent(EntryEvent<Integer, MyPortableElement> event) {
                eventLatch.countDown();
            }
        }, predicate, true);
        map.put(key, element);
        assertOpenEventually(eventLatch);

        Collection values = map.values(Predicates.lessThan("date", new Date().getTime()));
        assertEquals(values.iterator().next(), element);
    }

    @Test
    public void testPortable_query_with_index() {
        IMap<Integer, MyPortableElement> map = createMap();

        for (int i = 0; i < 100; i++) {
            MyPortableElement element = new MyPortableElement(i);
            map.put(i, element);
        }
        map.addIndex(IndexType.HASH, "id");
        Predicate predicate = or(
                equal("id", 0),
                equal("id", 1)
        );

        Collection values = map.values(predicate);
        assertEquals(2, values.size());
    }

    @Test
    public void testRemoveAllWithPredicate_DoesNotDeserializeValues() {
        IMap<Integer, MyPortableElement> map = createMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, new MyPortableElement(i));
        }

        Predicate<Integer, MyPortableElement> predicate = in("id", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        map.removeAll(predicate);

        for (int i = 0; i < 10; i++) {
            MyPortableElement entry = map.get(i);
            assertNull(entry);
        }

        for (int i = 10; i < 100; i++) {
            MyPortableElement entry = map.get(i);
            assertNotNull(entry);
        }
    }

    private static <K, V> IMap<K, V> createMap() {
        return client.getMap(randomString());
    }
}
