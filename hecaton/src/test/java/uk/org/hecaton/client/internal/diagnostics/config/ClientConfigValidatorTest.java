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

package uk.org.hecaton.client.internal.diagnostics.config;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientConfigValidatorTest extends HecatonTestSupport {

    private static final String MAP_NAME = "default";

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @Test
    public void getMap_throws_illegalArgumentException_whenLocalUpdatePolicy_is_cacheOnUpdate() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setLocalUpdatePolicy(CACHE_ON_UPDATE);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);

        factory.newHecatonInstance();
        HecatonInstance client = factory.newHecatonClient(clientConfig);

        assertThatThrownBy(() -> client.getMap(MAP_NAME))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Wrong `local-update-policy`");
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }
}
