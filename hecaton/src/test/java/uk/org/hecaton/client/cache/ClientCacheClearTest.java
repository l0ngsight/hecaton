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

package uk.org.hecaton.client.cache;

import uk.org.hecaton.cache.CacheClearTest;
import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
import uk.org.hecaton.client.impl.spi.EventHandler;
import uk.org.hecaton.client.impl.spi.impl.ListenerMessageCodec;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheClearTest extends CacheClearTest {

    private TestHecatonFactory clientFactory;
    private HecatonInstance client;

    @Override
    protected TestHecatonInstanceFactory getInstanceFactory(int instanceCount) {
        clientFactory = new TestHecatonFactory();
        return clientFactory;
    }

    protected ClientConfig createClientConfig() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig("myCache")
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setCacheLocalEntries(false)
                .setEvictionConfig(new EvictionConfig().setSize(10000)
                        .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT).setEvictionPolicy(EvictionPolicy.LFU))
                .setInvalidateOnChange(true)
                .setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE)
                .setMaxIdleSeconds(600)
                .setTimeToLiveSeconds(60);

        return new ClientConfig()
                .addNearCacheConfig(nearCacheConfig);
    }

    @Override
    protected void onSetup() {
        super.onSetup();
        ClientConfig clientConfig = createClientConfig();
        client = clientFactory.newHecatonClient(clientConfig);
    }

    @Test
    public void testClientInvalidationListenerCallCount() {
        ICache<String, String> cache = createCache();
        Map<String, String> entries = createAndFillEntries();

        for (Map.Entry<String, String> entry : entries.entrySet()) {
            cache.put(entry.getKey(), entry.getValue());
        }

        // Verify that put works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entries.get(key);
            String actualValue = cache.get(key);
            assertEquals(expectedValue, actualValue);
        }

        final AtomicInteger counter = new AtomicInteger();

        CacheConfig config = cache.getConfiguration(CacheConfig.class);

        registerInvalidationListener(event -> counter.getAndIncrement(), config.getNameWithPrefix());

        cache.clear();

        assertTrueEventually(() -> assertEquals(1, counter.get()));

        // Make sure that the callback is not called for a while
        assertTrueAllTheTime(() -> assertTrue(counter.get() <= 1), 3);

    }

    @Test
    public void testClientInvalidationListenerCallCountWhenServerCacheClearUsed() {
        ICache<String, String> cache = createCache();
        Map<String, String> entries = createAndFillEntries();

        for (Map.Entry<String, String> entry : entries.entrySet()) {
            cache.put(entry.getKey(), entry.getValue());
        }

        // Verify that put works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entries.get(key);
            String actualValue = cache.get(key);
            assertEquals(expectedValue, actualValue);
        }

        final AtomicInteger counter = new AtomicInteger();

        CacheConfig config = cache.getConfiguration(CacheConfig.class);

        registerInvalidationListener(event -> counter.getAndIncrement(), config.getNameWithPrefix());

        ICache<Object, Object> serverCache = getHecatonInstance().getCacheManager().getCache(config.getName());
        serverCache.clear();

        assertTrueEventually(() -> assertEquals(1, counter.get()));

        // Make sure that the callback is not called for a while
        assertTrueAllTheTime(() -> assertTrue(counter.get() <= 1), 3);

    }

    @Override
    protected void onTearDown() {
        clientFactory.shutdownAll();
        // Client factory is already shutdown at this test's super class (`CachePutAllTest`)
        // because it is returned instance factory from overridden `getInstanceFactory` method.
        client = null;
        super.onTearDown();
    }

    @Override
    protected CachingProvider getCachingProvider() {
        return createClientCachingProvider(client);
    }

    private void registerInvalidationListener(EventHandler handler, String nameWithPrefix) {
        ListenerMessageCodec listenerCodec = createInvalidationListenerCodec(nameWithPrefix);
        HecatonClientProxy hzClient = (HecatonClientProxy) client;
        final HecatonClientInstanceImpl clientInstance = hzClient.client;
        clientInstance.getListenerService().registerListener(listenerCodec, handler);
    }

    private ListenerMessageCodec createInvalidationListenerCodec(final String nameWithPrefix) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return CacheAddNearCacheInvalidationListenerCodec.encodeRequest(nameWithPrefix, localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return CacheAddNearCacheInvalidationListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return CacheRemoveEntryListenerCodec.encodeRequest(nameWithPrefix, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return CacheRemoveEntryListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

}
