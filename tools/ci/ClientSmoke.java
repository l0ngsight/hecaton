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

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.BuildInfoProvider;
import uk.org.hecaton.map.IMap;

/**
 * Smoke test for the Hecaton client library on the JDK it runs on (CI matrix: 17, 21, 25).
 * Connects to a member of cluster {@code client-smoke} at the given address (which runs on
 * Java 25, see tools/ci/client-tests.sh), writes a map entry with one client and reads it with
 * another.
 *
 * <pre>java -cp hecaton/target/hecaton-VERSION.jar tools/ci/ClientSmoke.java HOST:PORT</pre>
 *
 * Source-file mode keeps this runnable on JDK 17 without a build step.
 */
public class ClientSmoke {

    public static void main(String[] args) {
        HecatonInstance writer = client(args[0]);
        HecatonInstance reader = client(args[0]);
        int exitCode = 1;
        try {
            IMap<String, String> map = writer.getMap("smoke");
            map.put("key", "value");
            String read = reader.<String, String>getMap("smoke").get("key");
            if (!"value".equals(read)) {
                throw new AssertionError("expected 'value', read " + read);
            }
            System.out.println("Client smoke test passed: Hecaton "
                    + BuildInfoProvider.getBuildInfo().getProductVersion() + " client on Java " + Runtime.version());
            exitCode = 0;
        } finally {
            writer.shutdown();
            reader.shutdown();
        }
        System.exit(exitCode);
    }

    private static HecatonInstance client(String address) {
        ClientConfig config = new ClientConfig().setClusterName("client-smoke");
        config.getNetworkConfig().addAddress(address);
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(120_000);
        return HecatonClient.newHecatonClient(config);
    }
}
