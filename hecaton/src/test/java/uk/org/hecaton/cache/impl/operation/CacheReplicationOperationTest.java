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

package uk.org.hecaton.cache.impl.operation;

import uk.org.hecaton.cache.impl.CachePartitionSegment;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.internal.namespace.impl.NodeEngineThreadLocalContext;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheReplicationOperationTest extends HecatonTestSupport {

    @Test
    public void sendsConfigObjectOverWire() {
        // new config
        CacheConfig config = new CacheConfig("test-cache");

        // add config to cache service
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(createHecatonInstance());
        NodeEngineThreadLocalContext.declareNodeEngineReference(nodeEngineImpl);
        try {
            CacheService cacheService = nodeEngineImpl.getService(CacheService.SERVICE_NAME);
            cacheService.putCacheConfigIfAbsent(config);

            CachePartitionSegment segment = new CachePartitionSegment(cacheService, 0);
            segment.getOrCreateRecordStore(config.getNameWithPrefix());
            Collection<ServiceNamespace> namespaces = segment.getAllNamespaces(0);
            assertEquals(1, namespaces.size());

            // create operation
            CacheReplicationOperation operation = new CacheReplicationOperation();
            operation.prepare(segment, namespaces, 0);

            // serialize & deserialize operation
            Data data = nodeEngineImpl.toData(operation);
            CacheReplicationOperation cacheReplicationOperation = nodeEngineImpl.toObject(data);

            // new operation instance should have previously added config.
            assertContains(cacheReplicationOperation.getConfigs(), config);
        } finally {
            NodeEngineThreadLocalContext.destroyNodeEngineReference();
        }
    }
}
