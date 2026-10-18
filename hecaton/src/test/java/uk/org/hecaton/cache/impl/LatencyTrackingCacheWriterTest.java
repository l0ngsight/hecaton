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

package uk.org.hecaton.cache.impl;

import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.tenantcontrol.TenantContextual;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.diagnostics.StoreLatencyPlugin;
import uk.org.hecaton.spi.tenantcontrol.TenantControl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.integration.CacheWriter;
import java.util.Collection;
import java.util.LinkedList;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LatencyTrackingCacheWriterTest extends HecatonTestSupport {

    private static final String NAME = "someCache";

    private StoreLatencyPlugin plugin;
    private CacheWriter<Integer, String> delegate;
    private LatencyTrackingCacheWriter<Integer, String> cacheWriter;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        HecatonInstance hz = createHecatonInstance();
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        plugin = new StoreLatencyPlugin(nodeEngine.getLogger(StoreLatencyPlugin.class), nodeEngine.getProperties());
        delegate = mock(CacheWriter.class);
        TenantContextual<CacheWriter<Integer, String>> contextual = TenantContextual.create(() -> delegate,
                () -> true, TenantControl.NOOP_TENANT_CONTROL);
        cacheWriter = new LatencyTrackingCacheWriter<>(contextual, plugin, NAME);
    }

    @Test
    public void write() {
        Cache.Entry<Integer, String> entry = new CacheEntry<>(1, "peter");
        cacheWriter.write(entry);

        verify(delegate).write(entry);
        assertProbeCalledOnce("write");
    }

    @Test
    public void writeAll() {
        Collection<Cache.Entry<? extends Integer, ? extends String>> c = new LinkedList<>();

        cacheWriter.writeAll(c);

        verify(delegate).writeAll(c);
        assertProbeCalledOnce("writeAll");
    }

    @Test
    public void delete() {
        Cache.Entry<Integer, String> entry = new CacheEntry<>(1, "peter");
        cacheWriter.delete(entry);

        verify(delegate).delete(entry);
        assertProbeCalledOnce("delete");
    }

    @Test
    public void deleteAll() {
        Collection<Object> c = new LinkedList<>();

        cacheWriter.deleteAll(c);

        verify(delegate).deleteAll(c);
        assertProbeCalledOnce("deleteAll");
    }

    private void assertProbeCalledOnce(String methodName) {
        assertEquals(1, plugin.count(LatencyTrackingCacheWriter.KEY, NAME, methodName));
    }
}
