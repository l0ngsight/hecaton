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

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;

import java.util.ArrayList;
import java.util.List;

/** Hecaton members (and a client) in this JVM. */
final class HecatonGrid implements Grid {

    private static final int PORT = 5701;

    private final List<HecatonInstance> instances = new ArrayList<>();
    private IMap<Integer, byte[]> map;

    @Override
    public void start(int members, boolean client) {
        for (int i = 0; i < members; i++) {
            Config config = new Config().setClusterName("bench");
            config.getNetworkConfig().setPort(PORT).setPortAutoIncrement(true);
            JoinConfig join = config.getNetworkConfig().getJoin();
            join.getMulticastConfig().setEnabled(false);
            join.getAutoDetectionConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1:" + PORT + "-" + (PORT + members - 1));
            config.getJetConfig().setEnabled(false);
            instances.add(Hecaton.newHecatonInstance(config));
        }
        HecatonInstance entry = instances.get(0);
        if (client) {
            ClientConfig config = new ClientConfig().setClusterName("bench");
            config.getNetworkConfig().addAddress("127.0.0.1:" + PORT);
            entry = HecatonClient.newHecatonClient(config);
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
