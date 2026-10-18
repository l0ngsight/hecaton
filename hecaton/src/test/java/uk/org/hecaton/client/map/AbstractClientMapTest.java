/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.client.map;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonTestSupport;
import org.junit.After;
import org.junit.Before;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractClientMapTest extends HecatonTestSupport {

    protected TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    protected HecatonInstance client;

    protected HecatonInstance member1;
    protected HecatonInstance member2;

    @Before
    public final void startHecatonInstances() {
        Config config = getConfig();
        MapConfig mapConfig = new MapConfig("mapWithTTL");
        mapConfig.setTimeToLiveSeconds(1);
        config.addMapConfig(mapConfig);

        MapConfig mapConfig1 = new MapConfig("mapWithMaxIdle");
        mapConfig1.setMaxIdleSeconds(11);
        config.addMapConfig(mapConfig1);
        ClientConfig clientConfig = getClientConfig();

        member1 = hecatonFactory.newHecatonInstance(config);
        member2 = hecatonFactory.newHecatonInstance(config);

        client = hecatonFactory.newHecatonClient(clientConfig);
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfigWithoutJetAndMetrics();
    }

    @After
    public final void stopHecatonInstances() {
        hecatonFactory.terminateAll();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }
}
