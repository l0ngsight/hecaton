package compat;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.Collection;

/**
 * Writes test data to a Hazelcast cluster ({@code write ADDRESS CLUSTER}) and, after
 * {@code hecaton-migrate data} copied it, checks it in the Hecaton cluster
 * ({@code verify ADDRESS CLUSTER}), both with the official Hazelcast Java client.
 */
public final class DataCopyCompat {

    /** Serialized with Compact (zero-config for records); the copier does not have this class. */
    public record Account(long id, String owner, double balance) {
    }

    private static final int ENTRIES = 1500;

    private DataCopyCompat() {
    }

    public static void main(String[] args) {
        ClientConfig config = new ClientConfig();
        config.setClusterName(args[2]);
        config.getNetworkConfig().addAddress(args[1]);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        try {
            if (args[0].equals("write")) {
                write(client);
            } else {
                verify(client);
            }
            System.out.println("OK data-copy-" + args[0]);
        } finally {
            client.shutdown();
        }
    }

    private static void write(HazelcastInstance client) {
        IMap<Integer, String> plain = client.getMap("copy-plain");
        for (int i = 0; i < ENTRIES; i++) {
            plain.put(i, "value-" + i);
        }
        IMap<Long, Account> accounts = client.getMap("copy-accounts");
        accounts.put(1L, new Account(1, "alice", 10.5));
        accounts.put(2L, new Account(2, "bob", 20.0));
        client.getMultiMap("copy-tags").put("k", "a");
        client.getMultiMap("copy-tags").put("k", "b");
        client.getReplicatedMap("copy-settings").put("mode", "fast");
    }

    private static void verify(HazelcastInstance client) {
        IMap<Integer, String> plain = client.getMap("copy-plain");
        check(plain.size() == ENTRIES && "value-42".equals(plain.get(42)), "plain map: " + plain.size());
        IMap<Long, Account> accounts = client.getMap("copy-accounts");
        check(new Account(2, "bob", 20.0).equals(accounts.get(2L)), "compact record: " + accounts.get(2L));
        Collection<Object> tags = client.getMultiMap("copy-tags").get("k");
        check(tags.size() == 2 && tags.contains("a") && tags.contains("b"), "multimap: " + tags);
        check("fast".equals(client.getReplicatedMap("copy-settings").get("mode")), "replicated map");
    }

    private static void check(boolean condition, String what) {
        if (!condition) {
            throw new AssertionError(what);
        }
    }
}
