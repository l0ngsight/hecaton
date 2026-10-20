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
import uk.org.hecaton.map.IMap;

/**
 * Connects a client to a member at ADDRESS (default 127.0.0.1:5701, cluster "dev"), as the
 * container image starts it, and checks a map round trip.
 *
 * <pre>java -cp hecaton.jar tools/ci/ImageSmoke.java [ADDRESS]</pre>
 */
public final class ImageSmoke {

    private ImageSmoke() {
    }

    public static void main(String[] args) {
        ClientConfig config = new ClientConfig().setClusterName("dev");
        config.getNetworkConfig().addAddress(args.length > 0 ? args[0] : "127.0.0.1:5701");
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(60_000);
        HecatonInstance client = HecatonClient.newHecatonClient(config);
        try {
            IMap<String, String> map = client.getMap("image-smoke");
            map.put("key", "value");
            if (!"value".equals(map.get("key"))) {
                throw new AssertionError("map round trip failed");
            }
            System.out.println("OK image " + client.getCluster().getMembers());
        } finally {
            client.shutdown();
        }
    }
}
