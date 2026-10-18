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

package uk.org.hecaton.test;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import org.junit.runners.model.Statement;

import java.util.Collection;
import java.util.Set;

import static uk.org.hecaton.cache.jsr.JsrTestUtil.clearCachingProviderRegistry;
import static uk.org.hecaton.cache.jsr.JsrTestUtil.getCachingProviderRegistrySize;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;

class AfterClassesStatement extends Statement {
    private final Statement originalStatement;

    AfterClassesStatement(Statement originalStatement) {
        this.originalStatement = originalStatement;
    }

    @Override
    public void evaluate() throws Throwable {
        originalStatement.evaluate();

        // check for running Hecaton instances
        Set<HecatonInstance> instances = Hecaton.getAllHecatonInstances();
        if (!instances.isEmpty()) {
            String message = "Instances haven't been shut down: " + instances;
            HecatonInstanceFactory.terminateAll();
            assertTrueEventually(HecatonTestSupport::assertNoRunningInstances, 30);
            throw new IllegalStateException(message);
        }
        Collection<HecatonInstance> clientInstances = HecatonClient.getAllHecatonClients();
        if (!clientInstances.isEmpty()) {
            String message = "Client instances haven't been shut down: " + clientInstances;
            HecatonClient.shutdownAll();
            assertTrueEventually(HecatonTestSupport::assertNoRunningClientInstances, 30);
            throw new IllegalStateException(message);
        }

        // check for leftover JMX beans
        JmxLeakHelper.checkJmxBeans();

        // check for leftover CachingProvider instances
        int registrySize = getCachingProviderRegistrySize();
        if (registrySize > 0) {
            clearCachingProviderRegistry();
            throw new IllegalStateException(registrySize + " CachingProviders are not cleaned up."
                                            + " Please use JsrTestUtil.cleanup() in your test!");
        }
    }
}
