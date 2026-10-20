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

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.splitbrainprotection.PartitionedCluster;

import static uk.org.hecaton.client.splitbrainprotection.SplitBrainProtectionTestUtil.createClient;

public class PartitionedClusterClients {

    private TestHecatonFactory factory;
    private HecatonInstance[] clients;
    private PartitionedCluster cluster;

    public PartitionedClusterClients(PartitionedCluster cluster, TestHecatonFactory factory) {
        this.cluster = cluster;
        this.clients = new HecatonInstance[cluster.instance.length];
        this.factory = factory;
    }

    public HecatonInstance client(int index) {
        if (clients[index] == null) {
            clients[index] = createClient(factory, cluster.instance[index]);
        }
        return clients[index];
    }

    public void terminateAll() {
        for (HecatonInstance client : clients) {
            if (client != null) {
                client.shutdown();
            }
        }
    }
}
