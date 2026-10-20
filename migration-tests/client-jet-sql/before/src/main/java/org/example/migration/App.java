package org.example.migration;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.sql.SqlResult;

/**
 * Starts a member from hazelcast.yaml and a client from hazelcast-client.yaml, then runs a Jet
 * pipeline and an SQL query through the client.
 */
public final class App {

    private App() {
    }

    public static void main(String[] args) {
        Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        try {
            Pipeline pipeline = Pipeline.create();
            pipeline.readFrom(TestSources.items(1, 2, 3))
                    .writeTo(Sinks.map("squares", n -> n, n -> n * n));
            client.getJet().newJob(pipeline).join();

            client.getSql().execute("CREATE MAPPING squares TYPE IMap "
                    + "OPTIONS ('keyFormat' = 'int', 'valueFormat' = 'int')");
            try (SqlResult result = client.getSql().execute("SELECT SUM(this) FROM hazelcast.public.squares")) {
                long sum = result.iterator().next().getObject(0);
                check(sum == 14, "sum: " + sum);
            }

            System.out.println("OK client-jet-sql");
        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    private static void check(boolean condition, String what) {
        if (!condition) {
            throw new AssertionError(what);
        }
    }
}
