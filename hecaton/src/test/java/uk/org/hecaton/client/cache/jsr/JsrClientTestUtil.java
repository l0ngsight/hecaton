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

package uk.org.hecaton.client.cache.jsr;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;

import static uk.org.hecaton.cache.jsr.JsrTestUtil.assertNoMBeanLeftovers;
import static uk.org.hecaton.cache.jsr.JsrTestUtil.clearCachingProviderRegistry;
import static uk.org.hecaton.cache.jsr.JsrTestUtil.clearSystemProperties;
import static uk.org.hecaton.cache.jsr.JsrTestUtil.setSystemProperties;
import static uk.org.hecaton.test.HecatonTestSupport.assertThatIsNotMultithreadedTest;

/**
 * Utility class responsible for setup/cleanup of client JSR tests.
 */
public final class JsrClientTestUtil {

    private JsrClientTestUtil() {
    }

    public static void setup() {
        assertThatIsNotMultithreadedTest();
        setSystemProperties("client");
    }

    public static void setupWithHecatonInstance() {
        assertThatIsNotMultithreadedTest();
        setSystemProperties("client");
        assertNoMBeanLeftovers();

        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        Hecaton.newHecatonInstance(config);
    }

    public static void cleanup() {
        clearSystemProperties();
        clearCachingProviderRegistry();

        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
        HecatonInstanceFactory.terminateAll();
    }
}
