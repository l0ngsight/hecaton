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

package uk.org.hecaton.client.splitbrainprotection;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cluster.Address;

import static uk.org.hecaton.test.Accessors.getNode;

@SuppressWarnings("WeakerAccess")
public class SplitBrainProtectionTestUtil {

    private SplitBrainProtectionTestUtil() {
    }

    public static HecatonInstance createClient(TestHecatonFactory factory, HecatonInstance instance) {
        return factory.newHecatonClient(getClientConfig(instance));
    }

    public static ClientConfig getClientConfig(HecatonInstance instance) {
        ClientConfig clientConfig = new ClientConfig();
        Address address = getNode(instance).address;
        clientConfig.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        clientConfig.setClusterName(instance.getConfig().getClusterName());
        return clientConfig;
    }
}
