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

package uk.org.hecaton.benchmarks.fanout;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.config.SSLConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/** Hecaton members and clients in this JVM, see {@link ReadCluster}. */
final class HecatonReadCluster implements ReadCluster {

    private static final int PORT = 5701;

    private final List<HecatonInstance> members = new ArrayList<>();
    private final List<IMap<Integer, byte[]>> readers = new ArrayList<>();
    private final List<HecatonInstance> clients = new ArrayList<>();
    private final AtomicInteger shortLivedClients = new AtomicInteger();
    private final ExecutorService shutdowns = Executors.newCachedThreadPool();
    private Scenario scenario;
    private int hotKey;

    @Override
    public void start(Scenario scenario) {
        this.scenario = scenario;
        for (int i = 0; i < scenario.members(); i++) {
            Config config = new Config().setClusterName("fanout").setInstanceName("member-" + i);
            config.getNetworkConfig().setPort(PORT).setPortAutoIncrement(true);
            JoinConfig join = config.getNetworkConfig().getJoin();
            join.getMulticastConfig().setEnabled(false);
            join.getAutoDetectionConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(true);
            for (int port = PORT; port < PORT + scenario.members(); port++) {
                join.getTcpIpConfig().addMember("127.0.0.1:" + port);
            }
            if (scenario.tls()) {
                config.getNetworkConfig().setSSLConfig(ssl(scenario.keyStore()));
            }
            config.getJetConfig().setEnabled(false);
            config.getMapConfig(MAP).setInMemoryFormat(InMemoryFormat.valueOf(scenario.format()));
            if (scenario.nearCache() != null) {
                config.getMapConfig(MAP).setNearCacheConfig(new NearCacheConfig()
                        .setInMemoryFormat(InMemoryFormat.valueOf(scenario.nearCache())).setCacheLocalEntries(true));
            }
            members.add(Hecaton.newHecatonInstance(config));
        }
        HecatonInstance owner = members.get(0);
        hotKey = 0;
        while (!owner.getPartitionService().getPartition(hotKey).getOwner().localMember()) {
            hotKey++;
        }
        if (scenario.memberCaller()) {
            for (int i = 0; i < scenario.readers(); i++) {
                readers.add(members.get(1 + i % (members.size() - 1)).getMap(MAP));
            }
            return;
        }
        if (scenario.shortLivedClients()) {
            return;
        }
        for (int i = 0; i < scenario.readers(); i++) {
            HecatonInstance client = HecatonClient.newHecatonClient(clientConfig("reader-" + i));
            clients.add(client);
            readers.add(client.getMap(MAP));
        }
    }

    private ClientConfig clientConfig(String name) {
        ClientConfig config = new ClientConfig().setClusterName("fanout").setInstanceName(name);
        RoutingMode routing = RoutingMode.valueOf(scenario.routing());
        config.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(routing);
        List<HecatonInstance> targets = routing == RoutingMode.SINGLE_MEMBER ? members.subList(1, 2) : members;
        for (HecatonInstance member : targets) {
            config.getNetworkConfig().addAddress("127.0.0.1:" + member.getCluster().getLocalMember().getAddress().getPort());
        }
        if (scenario.tls()) {
            config.getNetworkConfig().setSSLConfig(ssl(scenario.keyStore()));
        }
        return config;
    }

    @Override
    public int hotKey() {
        return hotKey;
    }

    @Override
    public void put(int key, byte[] value) {
        members.get(0).<Integer, byte[]>getMap(MAP).set(key, value);
    }

    @Override
    public CompletionStage<byte[]> getAsync(int reader, int key) {
        if (scenario.shortLivedClients()) {
            HecatonInstance client = HecatonClient.newHecatonClient(clientConfig("reader-" + shortLivedClients.getAndIncrement()));
            return client.<Integer, byte[]>getMap(MAP).getAsync(key)
                    .whenCompleteAsync((value, error) -> client.shutdown(), shutdowns);
        }
        return readers.get(reader).getAsync(key);
    }

    @Override
    public byte[] get(int reader, int key) {
        if (scenario.shortLivedClients()) {
            HecatonInstance client = HecatonClient.newHecatonClient(clientConfig("reader-" + shortLivedClients.getAndIncrement()));
            try {
                return client.<Integer, byte[]>getMap(MAP).get(key);
            } finally {
                client.shutdown();
            }
        }
        return readers.get(reader).get(key);
    }

    @Override
    public void close() {
        shutdowns.shutdown();
        clients.forEach(HecatonInstance::shutdown);
        members.forEach(HecatonInstance::shutdown);
    }

    private static SSLConfig ssl(Path keyStore) {
        return new SSLConfig().setEnabled(true)
                .setProperty("keyStore", keyStore.toString())
                .setProperty("keyStorePassword", Scenario.KEY_STORE_PASSWORD)
                .setProperty("keyStoreType", "PKCS12")
                .setProperty("trustStore", keyStore.toString())
                .setProperty("trustStorePassword", Scenario.KEY_STORE_PASSWORD)
                .setProperty("trustStoreType", "PKCS12");
    }
}
