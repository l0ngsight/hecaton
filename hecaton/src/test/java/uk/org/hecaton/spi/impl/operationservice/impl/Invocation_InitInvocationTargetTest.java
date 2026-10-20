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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.impl.PartitionDataSerializerHook;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_TABLE_SEND_INTERVAL;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.PacketFiltersUtil.dropOperationsBetween;
import static uk.org.hecaton.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Collections.singletonList;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_InitInvocationTargetTest extends HecatonTestSupport {

    @Test
    public void testPartitionTableIsFetchedLazilyOnPartitionInvocation() throws ExecutionException, InterruptedException {
        Config config = new Config();
        config.setProperty(PARTITION_TABLE_SEND_INTERVAL.getName(), String.valueOf(Integer.MAX_VALUE));
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance[] instances = factory.newInstances(config);

        dropOperationsBetween(instances[0], instances[1], PartitionDataSerializerHook.F_ID,
                singletonList(PartitionDataSerializerHook.PARTITION_STATE_OP));

        OperationServiceImpl operationService = getNodeEngineImpl(instances[1]).getOperationService();
        Future<Object> future = operationService.invokeOnPartition(null, new DummyOperation(), 0);

        resetPacketFiltersFrom(instances[0]);

        future.get();
    }
}
