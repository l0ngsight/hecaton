/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.client.cp.internal.datastructures.lock;

import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.internal.datastructures.lock.UnsafeFencedLockBasicTest;
import uk.org.hecaton.cp.internal.session.AbstractProxySessionManager;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeFencedLockClientBasicTest extends UnsafeFencedLockBasicTest {

    private TestHecatonFactory factory = new TestHecatonFactory();
    private HecatonInstance client;

    @Override
    protected TestHecatonInstanceFactory createTestFactory() {
        return factory;
    }

    @Override
    protected HecatonInstance[] createInstances() {
        HecatonInstance[] instances = super.createInstances();
        client = factory.newHecatonClient();
        return instances;
    }

    @Override
    protected HecatonInstance getProxyInstance() {
        return client;
    }

    @Test
    public void test_lockAutoRelease_onClientShutdown() {
        String proxyName = lock.getName();
        lock.lock();

        client.shutdown();

        assertFalse(instances[0].getCPSubsystem().getLock(proxyName).isLocked());
    }

    @Override
    protected AbstractProxySessionManager getSessionManager(HecatonInstance instance) {
        return (AbstractProxySessionManager) (((HecatonClientProxy) client).client).getProxySessionManager();
    }

}
