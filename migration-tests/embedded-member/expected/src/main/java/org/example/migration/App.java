package org.example.migration;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.IMap;

import java.util.Map;

/** Starts a member from hazelcast.xml, runs an entry processor and checks the configuration. */
public final class App {

    private App() {
    }

    public static void main(String[] args) {
        HecatonInstance hazelcastInstance = Hecaton.newHecatonInstance();
        try {
            Config config = hazelcastInstance.getConfig();
            check("migration-test".equals(config.getClusterName()), "cluster name: " + config.getClusterName());
            check("jdk".equals(config.getProperty("hecaton.logging.type")), "logging type property");
            check(config.getMapConfig("hazelcast-counters").getBackupCount() == 0, "map config");

            IMap<String, Integer> counters = hazelcastInstance.getMap("hazelcast-counters");
            counters.put("visits", 41);
            counters.executeOnKey("visits", new Increment());
            check(counters.get("visits") == 42, "visits: " + counters.get("visits"));

            System.out.println("OK embedded-member");
        } finally {
            Hecaton.shutdownAll();
        }
    }

    private static void check(boolean condition, String what) {
        if (!condition) {
            throw new AssertionError(what);
        }
    }

    /** An entry processor that also gets the member injected. */
    static final class Increment implements EntryProcessor<String, Integer, Integer>, HecatonInstanceAware {

        private transient HecatonInstance hazelcastInstance;

        @Override
        public Integer process(Map.Entry<String, Integer> entry) {
            entry.setValue(entry.getValue() + 1);
            return entry.getValue();
        }

        @Override
        public void setHecatonInstance(HecatonInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }
    }
}
