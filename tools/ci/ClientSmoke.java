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
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.BuildInfoProvider;
import uk.org.hecaton.map.IMap;

/**
 * Smoke test for the Hecaton client library on the JDK it runs on (CI matrix: 17, 21, 25).
 * Starts one member and one client in this JVM and round-trips a map entry.
 *
 * <pre>java -cp hecaton/target/hecaton-VERSION.jar tools/ci/ClientSmoke.java</pre>
 *
 * Source-file mode keeps this runnable on JDK 17 without a build step.
 */
public class ClientSmoke {

    public static void main(String[] args) {
        String cluster = "client-smoke";
        Config config = new Config().setClusterName(cluster);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);

        HecatonInstance member = Hecaton.newHecatonInstance(config);
        int exitCode = 1;
        try {
            int port = member.getCluster().getLocalMember().getAddress().getPort();
            ClientConfig clientConfig = new ClientConfig().setClusterName(cluster);
            clientConfig.getNetworkConfig().addAddress("127.0.0.1:" + port);
            HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);
            try {
                IMap<String, String> map = client.getMap("smoke");
                map.put("key", "value");
                String fromMember = member.<String, String>getMap("smoke").get("key");
                if (!"value".equals(fromMember)) {
                    throw new AssertionError("expected 'value' on the member, got " + fromMember);
                }
                System.out.println("Client smoke test passed: Hecaton "
                        + BuildInfoProvider.getBuildInfo().getVersion() + " on Java " + Runtime.version());
                exitCode = 0;
            } finally {
                client.shutdown();
            }
        } finally {
            member.shutdown();
        }
        System.exit(exitCode);
    }
}
