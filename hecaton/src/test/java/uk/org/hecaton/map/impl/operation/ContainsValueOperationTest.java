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

package uk.org.hecaton.map.impl.operation;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ContainsValueOperationTest extends HecatonTestSupport {

    private final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
    private HecatonInstance member1;

    @Before
    public void setUp() {
        Config config = getConfig();
        member1 = factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
    }

    @Test
    public void test_ContainValueOperation() throws ExecutionException, InterruptedException {
        int value = 1;
        String mapName = randomMapName();

        IMap<String, Integer> map = member1.getMap(mapName);
        String key = generateKeyNotOwnedBy(member1);
        map.put(key, value);

        Future future = executeOperation(map, key, value);

        assertTrue((Boolean) future.get());
    }

    private InternalCompletableFuture<Object> executeOperation(Map map, String key, int value) {
        int partitionId = getNode(member1).getPartitionService().getPartitionId(key);
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapServiceContext mapServiceContext = ((MapService) mapProxy.getService()).getMapServiceContext();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapProxy.getName());
        OperationFactory operationFactory
                = operationProvider.createContainsValueOperationFactory(mapProxy.getName(), mapServiceContext.toData(value));
        Operation operation = operationFactory.createOperation();

        OperationServiceImpl operationService = getOperationService(member1);
        return operationService.createInvocationBuilder(MapService.SERVICE_NAME, operation, partitionId).invoke();
    }
}
