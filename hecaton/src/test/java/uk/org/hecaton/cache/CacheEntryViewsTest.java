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

import uk.org.hecaton.cache.impl.CacheEntryViews;
import uk.org.hecaton.cache.impl.record.CacheObjectRecord;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheEntryViewsTest extends HecatonTestSupport {

    private SerializationService serializationService;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void testPrivateConstructor() {
        HecatonTestSupport.assertUtilityConstructor(CacheEntryViews.class);
    }

    private void doCacheEntryViewTest(CacheEntryViews.CacheEntryViewType cacheEntryViewType) {
        String key = "testKey";
        String value = "testValue";
        ExpiryPolicy expiryPolicy = new EternalExpiryPolicy();
        CacheObjectRecord record = new CacheObjectRecord(value, System.currentTimeMillis(), 1234L);
        record.setExpiryPolicy(new EternalExpiryPolicy());
        CacheEntryView cacheEntryView =
                CacheEntryViews.createEntryView(serializationService.toData(key),
                        serializationService.toData(value),
                        serializationService.toData(expiryPolicy),
                        record,
                        cacheEntryViewType);

        assertEquals(key, serializationService.toObject(cacheEntryView.getKey()));
        assertEquals(value, serializationService.toObject(cacheEntryView.getValue()));
        assertEquals(record.getHits(), cacheEntryView.getHits());
        assertEquals(record.getExpirationTime(), cacheEntryView.getExpirationTime());
        assertEquals(record.getLastAccessTime(), cacheEntryView.getLastAccessTime());
        assertInstanceOf(EternalExpiryPolicy.class, serializationService.toObject(cacheEntryView.getExpiryPolicy()));
    }

    @Test
    public void testDefaultCacheEntryView() {
        doCacheEntryViewTest(CacheEntryViews.CacheEntryViewType.DEFAULT);
    }

    @Test
    public void testLazyCacheEntryView() {
        doCacheEntryViewTest(CacheEntryViews.CacheEntryViewType.LAZY);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCacheEntryView() {
        doCacheEntryViewTest(null);
    }

}
