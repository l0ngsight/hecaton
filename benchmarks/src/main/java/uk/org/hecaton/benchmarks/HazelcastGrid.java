/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.benchmarks;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.ArrayList;
import java.util.List;

/** Hazelcast CE members (and a client) in this JVM, configured like {@link HecatonGrid}. */
final class HazelcastGrid implements Grid {

    private static final int PORT = 5801;

    private final List<HazelcastInstance> instances = new ArrayList<>();
    private IMap<Integer, byte[]> map;

    @Override
    public void start(int members, boolean client) {
        System.setProperty("hazelcast.phone.home.enabled", "false");
        for (int i = 0; i < members; i++) {
            Config config = new Config().setClusterName("bench");
            config.getNetworkConfig().setPort(PORT).setPortAutoIncrement(true);
            JoinConfig join = config.getNetworkConfig().getJoin();
            join.getMulticastConfig().setEnabled(false);
            join.getAutoDetectionConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1:" + PORT + "-" + (PORT + members - 1));
            config.getJetConfig().setEnabled(false);
            instances.add(Hazelcast.newHazelcastInstance(config));
        }
        HazelcastInstance entry = instances.get(0);
        if (client) {
            ClientConfig config = new ClientConfig().setClusterName("bench");
            config.getNetworkConfig().addAddress("127.0.0.1:" + PORT);
            entry = HazelcastClient.newHazelcastClient(config);
            instances.add(entry);
        }
        map = entry.getMap("bench");
    }

    @Override
    public byte[] get(int key) {
        return map.get(key);
    }

    @Override
    public void set(int key, byte[] value) {
        map.set(key, value);
    }

    @Override
    public byte[] put(int key, byte[] value) {
        return map.put(key, value);
    }

    @Override
    public void close() {
        for (int i = instances.size() - 1; i >= 0; i--) {
            instances.get(i).shutdown();
        }
    }
}
