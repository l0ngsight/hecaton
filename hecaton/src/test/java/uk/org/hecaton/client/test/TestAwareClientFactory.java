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

package uk.org.hecaton.client.test;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientNetworkConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.test.TestAwareInstanceFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static uk.org.hecaton.config.Config.DEFAULT_CLUSTER_NAME;
import static uk.org.hecaton.test.AbstractHecatonClassRunner.getTestMethodName;

/**
 * Per test-method factory for Hecaton clients (and also members as it inherits from {@link TestAwareInstanceFactory}).
 * It configures new clients in the same way as it's done for members in
 * {@link TestAwareInstanceFactory#newHecatonInstance(uk.org.hecaton.config.Config)}.
 * <p>
 * <b>Tests using this factory should not be annotated with {@code ParallelJVMTest} category to avoid runs in multiple JVMs.</b>
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class TestAwareClientFactory extends TestAwareInstanceFactory {

    protected final Map<String, List<HecatonInstance>> perMethodClients = new ConcurrentHashMap<>();

    /**
     * Creates new client instance which uses in its network configuration the first member created by this factory. The value
     * {@link uk.org.hecaton.test.AbstractHecatonClassRunner#getTestMethodName()} is used as the cluster name if it's not
     * changed already.
     */
    public HecatonInstance newHecatonClient(ClientConfig config) {
        if (config == null) {
            config = new ClientConfig();
        }
        if (DEFAULT_CLUSTER_NAME.equals(config.getClusterName())) {
            config.setClusterName(getTestMethodName());
        }
        List<HecatonInstance> members = getOrInitInstances(perMethodMembers);
        if (members.isEmpty()) {
            throw new IllegalStateException("Members have to be created first");
        }
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        for (HecatonInstance member : members) {
            networkConfig.addAddress("127.0.0.1:" + getPort(member, EndpointQualifier.CLIENT));
        }
        HecatonInstance hz = HecatonClient.newHecatonClient(config);
        getOrInitInstances(perMethodClients).add(hz);
        return hz;
    }

    /**
     * Terminates all client and member instances created by this factory for current test method name.
     */
    @Override
    public void terminateAll() {
        shutdownInstances(perMethodClients.remove(getTestMethodName()));
        super.terminateAll();
    }
}
