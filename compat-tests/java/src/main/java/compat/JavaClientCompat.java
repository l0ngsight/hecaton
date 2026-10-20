package compat;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.topic.ITopic;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Runs basic scenarios with the official Hazelcast Java client against a Hecaton member:
 * IMap with an entry listener, queue, topic, SQL and Compact serialization. Exits non-zero
 * (with an AssertionError) on the first failure.
 */
public final class JavaClientCompat {

    /** Serialized with Compact (zero-config for records). */
    public record Order(long id, String customer, double amount) {
    }

    private JavaClientCompat() {
    }

    public static void main(String[] args) throws Exception {
        String prefix = args.length > 0 ? args[0] : "java";
        ClientConfig config = new ClientConfig();
        config.setClusterName("compat");
        config.getNetworkConfig().addAddress("127.0.0.1:5961");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        try {
            map(client, prefix);
            queue(client, prefix);
            topic(client, prefix);
            sqlAndCompact(client, prefix);
            System.out.println("OK " + prefix);
        } finally {
            client.shutdown();
        }
    }

    private static void map(HazelcastInstance client, String prefix) throws InterruptedException {
        IMap<String, Integer> map = client.getMap(prefix + "-map");
        BlockingQueue<String> added = new ArrayBlockingQueue<>(10);
        map.addEntryListener(new EntryAdapter<String, Integer>() {
            @Override
            public void entryAdded(EntryEvent<String, Integer> event) {
                // events of different keys may arrive in any order
                if ("a".equals(event.getKey())) {
                    added.add(event.getKey());
                }
            }
        }, true);
        map.put("a", 1);
        map.put("b", 2);
        check(map.get("a") == 1 && map.size() == 2, "map put/get/size");
        check("a".equals(added.poll(30, TimeUnit.SECONDS)), "entry listener");
    }

    private static void queue(HazelcastInstance client, String prefix) {
        IQueue<String> queue = client.getQueue(prefix + "-queue");
        queue.offer("first");
        queue.offer("second");
        check("first".equals(queue.poll()) && queue.size() == 1, "queue offer/poll");
    }

    private static void topic(HazelcastInstance client, String prefix) throws InterruptedException {
        ITopic<String> topic = client.getTopic(prefix + "-topic");
        BlockingQueue<String> received = new ArrayBlockingQueue<>(10);
        topic.addMessageListener(message -> received.add(message.getMessageObject()));
        topic.publish("hello");
        check("hello".equals(received.poll(30, TimeUnit.SECONDS)), "topic publish/listen");
    }

    private static void sqlAndCompact(HazelcastInstance client, String prefix) {
        String mapName = prefix.replaceAll("[^A-Za-z0-9]", "_") + "_orders";
        IMap<Long, Order> orders = client.getMap(mapName);
        orders.put(1L, new Order(1, "alice", 10.5));
        orders.put(2L, new Order(2, "bob", 20.0));
        check(orders.get(2L).customer().equals("bob"), "compact get");

        client.getSql().execute("CREATE OR REPLACE MAPPING " + mapName + " (id BIGINT, customer VARCHAR, amount DOUBLE) "
                + "TYPE IMap OPTIONS ('keyFormat' = 'bigint', 'valueFormat' = 'compact', "
                + "'valueCompactTypeName' = '" + Order.class.getName() + "')");
        try (SqlResult result = client.getSql().execute(
                "SELECT customer FROM " + mapName + " WHERE amount > ? ORDER BY id", 15.0)) {
            SqlRow row = result.iterator().next();
            check("bob".equals(row.getObject("customer")), "sql select");
        }
    }

    private static void check(boolean condition, String what) {
        if (!condition) {
            throw new AssertionError(what);
        }
    }
}
