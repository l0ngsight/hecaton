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

package uk.org.hecaton.spi.impl.operationservice.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.operation.PutOperation;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.test.Accessors.getNode;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationServiceImpl_asyncInvokeOnPartitionTest extends HecatonTestSupport {

    @Test
    public void testAsyncInvocation_SamePartitionThread_DifferentPartition() {
        HecatonInstance instance = createHecatonInstance();
        String name = randomString();
        String key = randomString();
        IMap<String, String> map = instance.getMap(name);
        map.put(key, name);
        map.executeOnKey(key, new InvocationEntryProcessor());
        assertOpenEventually(InvocationEntryProcessor.latch, 10);
    }

    public static class InvocationEntryProcessor implements EntryProcessor, HecatonInstanceAware {

        public static final CountDownLatch latch = new CountDownLatch(1);

        transient HecatonInstance instance;

        @Override
        public Object process(Map.Entry entry) {
            Node node = getNode(instance);
            NodeEngineImpl nodeEngine = node.nodeEngine;
            OperationServiceImpl operationService = nodeEngine.getOperationService();
            Data sourceKey = nodeEngine.toData(entry.getKey());
            Data key = generateKey_FallsToSamePartitionThread_ButDifferentPartition(nodeEngine, sourceKey);
            Data val = nodeEngine.toData(randomString());
            PutOperation op = new PutOperation((String) entry.getValue(), key, val);
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            operationService.invokeOnPartitionAsync(MapService.SERVICE_NAME, op, partitionId)
                    .thenRun(latch::countDown);
            return null;
        }

        private Data generateKey_FallsToSamePartitionThread_ButDifferentPartition(NodeEngineImpl nodeEngine, Data sourceKey) {
            InternalPartitionService partitionService = nodeEngine.getPartitionService();
            int sourcePartitionId = partitionService.getPartitionId(sourceKey);
            OperationServiceImpl operationService = nodeEngine.getOperationService();
            int threadCount = operationService.operationExecutor.getPartitionThreadCount();
            int sourceThreadId = sourcePartitionId % threadCount;

            while (true) {
                Data key = nodeEngine.toData(randomString());
                int partitionId = partitionService.getPartitionId(key);
                int threadId = partitionId % threadCount;
                if (sourcePartitionId != partitionId && sourceThreadId == threadId) {
                    return key;
                }
            }
        }

        @Override
        public EntryProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            instance = hecatonInstance;
        }
    }
}
