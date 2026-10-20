package org.example.migration;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;

import java.util.Map;

/** Starts a member from hazelcast.xml, runs an entry processor and checks the configuration. */
public final class App {

    private App() {
    }

    public static void main(String[] args) {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
        try {
            Config config = hazelcastInstance.getConfig();
            check("migration-test".equals(config.getClusterName()), "cluster name: " + config.getClusterName());
            check("jdk".equals(config.getProperty("hazelcast.logging.type")), "logging type property");
            check(config.getMapConfig("hazelcast-counters").getBackupCount() == 0, "map config");

            IMap<String, Integer> counters = hazelcastInstance.getMap("hazelcast-counters");
            counters.put("visits", 41);
            counters.executeOnKey("visits", new Increment());
            check(counters.get("visits") == 42, "visits: " + counters.get("visits"));

            System.out.println("OK embedded-member");
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    private static void check(boolean condition, String what) {
        if (!condition) {
            throw new AssertionError(what);
        }
    }

    /** An entry processor that also gets the member injected. */
    static final class Increment implements EntryProcessor<String, Integer, Integer>, HazelcastInstanceAware {

        private transient HazelcastInstance hazelcastInstance;

        @Override
        public Integer process(Map.Entry<String, Integer> entry) {
            entry.setValue(entry.getValue() + 1);
            return entry.getValue();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }
    }
}
