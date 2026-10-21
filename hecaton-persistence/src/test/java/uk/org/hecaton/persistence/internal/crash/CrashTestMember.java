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
package uk.org.hecaton.persistence.internal.crash;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DataPersistenceConfig;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.persistence.internal.PersistenceModule;
import uk.org.hecaton.spi.properties.ClusterProperty;

import java.io.File;
import java.io.PrintStream;

/**
 * A member in its own JVM for {@link CrashConsistencyTest}. With {@code write}, it writes to the
 * map "persistent" one operation at a time (see {@link #operation}) and prints {@code ACK <seq>}
 * once each has returned, until the JVM is killed.
 */
public final class CrashTestMember {

    static final String MAP = "persistent";
    static final int KEYS = 300;
    static final int BASE_PORT = 5951;
    private static final int DELETE_EVERY = 5;
    private static final int KEY_STRIDE = 7;

    private CrashTestMember() {
    }

    /** @param args base directory, member count, {@code write} or {@code idle} */
    public static void main(String[] args) {
        PrintStream out = System.out;
        HecatonInstance member = Hecaton.newHecatonInstance(config(new File(args[0]), Integer.parseInt(args[1])));
        out.println("READY " + member.getCluster().getLocalMember().getUuid());
        out.flush();
        if ("write".equals(args[2])) {
            IMap<Integer, Integer> map = member.getMap(MAP);
            for (long seq = 0; ; seq++) {
                Operation operation = operation(seq);
                if (operation.put()) {
                    map.set(operation.key(), (int) seq);
                } else {
                    map.delete(operation.key());
                }
                out.println("ACK " + seq);
                out.flush();
            }
        }
    }

    /** The operation with the given sequence number: mostly puts, every fifth a delete. */
    static Operation operation(long seq) {
        if (seq % DELETE_EVERY == DELETE_EVERY - 1) {
            return new Operation(false, (int) ((seq * KEY_STRIDE) % KEYS));
        }
        return new Operation(true, (int) (seq % KEYS));
    }

    static Config config(File baseDir, int memberCount) {
        Config config = new Config();
        config.setClusterName("crash-test");
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "31");
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "1");
        // the member's output carries the acknowledgements
        config.setProperty(ClusterProperty.LOGGING_TYPE.getName(), "none");
        // small chunks: the writes roll chunks and run compactions all the time
        config.setProperty(PersistenceModule.MAX_CHUNK_SIZE.getName(), "16384");
        config.getNetworkConfig().setPort(BASE_PORT).setPortAutoIncrement(true);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        for (int i = 0; i < memberCount; i++) {
            join.getTcpIpConfig().addMember("127.0.0.1:" + (BASE_PORT + i));
        }
        config.getPersistenceConfig().setEnabled(true).setBaseDir(baseDir)
                .setValidationTimeoutSeconds(120).setDataLoadTimeoutSeconds(120);
        config.addMapConfig(new MapConfig(MAP).setBackupCount(1)
                .setDataPersistenceConfig(new DataPersistenceConfig().setEnabled(true)));
        return config;
    }

    /** A put of the sequence number, or a delete. */
    record Operation(boolean put, int key) {
    }
}
