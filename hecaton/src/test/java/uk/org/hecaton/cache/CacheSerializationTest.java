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

package uk.org.hecaton.cache;

import uk.org.hecaton.cache.impl.CachePartitionEventData;
import uk.org.hecaton.cache.impl.CachePartitionSegment;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.operation.CacheReplicationOperation;
import uk.org.hecaton.cache.impl.record.CacheRecord;
import uk.org.hecaton.cache.impl.record.CacheRecordFactory;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.instance.impl.HecatonInstanceProxy;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.SerializationServiceBuilder;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.internal.util.CollectionUtil;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.version.MemberVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.Collection;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static org.junit.Assert.assertEquals;

/**
 * Serialization test class for JCache
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheSerializationTest extends HecatonTestSupport {

    SerializationService service;

    @Before
    public void setup() {
        SerializationServiceBuilder builder = new DefaultSerializationServiceBuilder();
        service = builder.build();
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testCacheRecord_withBinaryInMemoryData() {
        String value = randomString();
        CacheRecord cacheRecord = createRecord(InMemoryFormat.BINARY, value);

        Data cacheRecordData = service.toData(cacheRecord);
        CacheRecord deserialized = service.toObject(cacheRecordData);
        assertEquals(value, service.toObject(deserialized.getValue()));
    }

    @Test
    public void testCacheRecord_withObjectInMemoryData() {
        String value = randomString();
        CacheRecord cacheRecord = createRecord(InMemoryFormat.OBJECT, value);

        Data cacheRecordData = service.toData(cacheRecord);
        CacheRecord deserialized = service.toObject(cacheRecordData);
        assertEquals(value, deserialized.getValue());
    }

    @Test
    public void test_CacheReplicationOperation_serialization() throws Exception {
        TestHecatonInstanceFactory factory = new TestHecatonInstanceFactory(1);
        HecatonInstance hecatonInstance = factory.newHecatonInstance();

        try {
            CachingProvider provider = createServerCachingProvider(hecatonInstance);
            CacheManager manager = provider.getCacheManager();

            CompleteConfiguration configuration = new MutableConfiguration();
            Cache cache1 = manager.createCache("cache1", configuration);
            Cache cache2 = manager.createCache("cache2", configuration);
            Cache cache3 = manager.createCache("cache3", configuration);

            for (int i = 0; i < 1000; i++) {
                cache1.put("key" + i, i);
                cache2.put("key" + i, i);
                cache3.put("key" + i, i);
            }

            HecatonInstanceProxy proxy = (HecatonInstanceProxy) hecatonInstance;

            Field original = HecatonInstanceProxy.class.getDeclaredField("original");
            original.setAccessible(true);

            HecatonInstanceImpl impl = (HecatonInstanceImpl) original.get(proxy);
            NodeEngineImpl nodeEngine = impl.node.nodeEngine;
            CacheService cacheService = nodeEngine.getService(CacheService.SERVICE_NAME);

            int partitionCount = nodeEngine.getPartitionService().getPartitionCount();

            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                CachePartitionSegment segment = cacheService.getSegment(partitionId);

                int replicaIndex = 1;
                Collection<ServiceNamespace> namespaces = segment.getAllNamespaces(replicaIndex);
                if (CollectionUtil.isEmpty(namespaces)) {
                    continue;
                }

                CacheReplicationOperation operation = new CacheReplicationOperation();
                operation.prepare(segment, namespaces, replicaIndex);
                Data serialized = service.toData(operation);
                try {
                    service.toObject(serialized);
                } catch (Exception e) {
                    throw new Exception("Partition: " + partitionId, e);
                }
            }

        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void testCachePartitionEventData() throws UnknownHostException {
        Address address = new Address("127.0.0.1", 5701);
        Member member = new MemberImpl(address, MemberVersion.UNKNOWN, true);
        CachePartitionEventData cachePartitionEventData = new CachePartitionEventData("test", 1, member);
        CachePartitionEventData deserialized = service.toObject(cachePartitionEventData);
        assertEquals(cachePartitionEventData, deserialized);
    }

    private CacheRecord createRecord(InMemoryFormat format, String value) {
        CacheRecordFactory factory = new CacheRecordFactory(format, service);
        return factory.newRecordWithExpiry(value, Clock.currentTimeMillis(), -1);
    }

}
