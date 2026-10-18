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

package uk.org.hecaton.client.cache.instance;

import uk.org.hecaton.cache.instance.CacheThroughHecatonInstanceTest;
import uk.org.hecaton.client.HecatonClientNotActiveException;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;

import java.util.UUID;

import static uk.org.hecaton.cache.CacheTestSupport.createClientCachingProvider;
import static uk.org.hecaton.client.test.ClientTestSupport.makeSureDisconnectedFromServer;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheThroughHecatonInstanceTest extends CacheThroughHecatonInstanceTest {

    private TestHecatonFactory instanceFactory;
    private HecatonInstance ownerInstance;

    @Override
    protected CachingProvider createCachingProvider(HecatonInstance instance) {
        return createClientCachingProvider(instance);
    }

    @Override
    protected HecatonInstance createInstance() {
        if (instanceFactory != null) {
            throw new IllegalStateException("There should not be multiple creation of TestHecatonFactory!");
        }
        instanceFactory = new TestHecatonFactory();
        ownerInstance = instanceFactory.newHecatonInstance();
        return instanceFactory.newHecatonClient();
    }

    @Override
    protected HecatonInstance createInstance(Config config) {
        if (instanceFactory != null) {
            throw new IllegalStateException("There should not be multiple creation of TestHecatonFactory!");
        }
        instanceFactory = new TestHecatonFactory();
        ownerInstance = instanceFactory.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(10_000);

        if (config.getClassLoader() != null) {
            final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
            try {
                clientConfig.setClassLoader(config.getClassLoader());
                Thread.currentThread().setContextClassLoader(config.getClassLoader());
                return instanceFactory.newHecatonClient(clientConfig);
            } finally {
                Thread.currentThread().setContextClassLoader(tccl);
            }
        } else {
            return instanceFactory.newHecatonClient(clientConfig);
        }
    }

    @Override
    protected void shutdownOwnerInstance(HecatonInstance instance) {
        if (ownerInstance != null) {
            UUID memberUuid = ownerInstance.getLocalEndpoint().getUuid();
            ownerInstance.shutdown();
            makeSureDisconnectedFromServer(instance, memberUuid);
        } else {
            throw new IllegalStateException("");
        }
    }

    @Override
    protected Class<? extends Exception> getInstanceNotActiveExceptionType() {
        return HecatonClientNotActiveException.class;
    }

    @After
    public void tearDown() {
        if (instanceFactory != null) {
            ownerInstance = null;
            instanceFactory.shutdownAll();
            instanceFactory = null;
        }
    }

}
