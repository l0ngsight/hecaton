package org.example.migration;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.test.TestSources;
import uk.org.hecaton.sql.SqlResult;

/**
 * Starts a member from hazelcast.yaml and a client from hazelcast-client.yaml, then runs a Jet
 * pipeline and an SQL query through the client.
 */
public final class App {

    private App() {
    }

    public static void main(String[] args) {
        Hecaton.newHecatonInstance();
        HecatonInstance client = HecatonClient.newHecatonClient();
        try {
            Pipeline pipeline = Pipeline.create();
            pipeline.readFrom(TestSources.items(1, 2, 3))
                    .writeTo(Sinks.map("squares", n -> n, n -> n * n));
            client.getJet().newJob(pipeline).join();

            client.getSql().execute("CREATE MAPPING squares TYPE IMap "
                    + "OPTIONS ('keyFormat' = 'int', 'valueFormat' = 'int')");
            try (SqlResult result = client.getSql().execute("SELECT SUM(this) FROM hecaton.public.squares")) {
                long sum = result.iterator().next().getObject(0);
                check(sum == 14, "sum: " + sum);
            }

            System.out.println("OK client-jet-sql");
        } finally {
            HecatonClient.shutdownAll();
            Hecaton.shutdownAll();
        }
    }

    private static void check(boolean condition, String what) {
        if (!condition) {
            throw new AssertionError(what);
        }
    }
}
