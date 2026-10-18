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

package uk.org.hecaton.instance.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.NetworkConfig;
import uk.org.hecaton.instance.TestNodeContext;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InOrder;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NodeExtensionTest extends HecatonTestSupport {

    private HecatonInstanceImpl hecatonInstance;

    @After
    public void cleanup() {
        if (hecatonInstance != null) {
            hecatonInstance.shutdown();
        }
    }

    @Test
    public void verifyMethods() throws Exception {
        TestNodeContext nodeContext = new TestNodeContext();
        NodeExtension nodeExtension = nodeContext.getNodeExtension();

        hecatonInstance = new HecatonInstanceImpl(randomName(), getConfig(), nodeContext);

        InOrder inOrder = inOrder(nodeExtension);

        inOrder.verify(nodeExtension, times(1)).printNodeInfo();
        inOrder.verify(nodeExtension, times(1)).beforeStart();
        inOrder.verify(nodeExtension, times(1)).createSerializationService();
        inOrder.verify(nodeExtension, times(1)).createExtensionServices();
        inOrder.verify(nodeExtension, times(1)).beforeJoin();
        inOrder.verify(nodeExtension, times(1)).afterStart();

        hecatonInstance.shutdown();
        inOrder.verify(nodeExtension, times(1)).beforeShutdown(false);
        inOrder.verify(nodeExtension, times(1)).shutdown();
    }

    protected Config getConfig() {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        return config;
    }
}
