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
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationServiceImpl_invokeOnTargetLiteMemberTest
        extends HecatonTestSupport {

    private Config liteMemberConfig = new Config().setLiteMember(true);

    private Operation operation;

    @Before
    public void before() {
        operation = new DummyOperation("foobar");
    }

    @Test
    public void test_invokeTarget_onLiteMember()
            throws InterruptedException, ExecutionException {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final HecatonInstance instance = factory.newHecatonInstance(liteMemberConfig);

        final OperationServiceImpl operationService = getOperationService(instance);
        final InternalCompletableFuture<Object> future = operationService.invokeOnTarget(null, operation, getAddress(instance));

        assertEquals("foobar", future.get());
    }

}
