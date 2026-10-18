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

package uk.org.hecaton.dataconnection;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.dataconnection.impl.hecatondataconnection.HecatonDataConnectionClientConfigBuilder;
import uk.org.hecaton.dataconnection.impl.hecatondataconnection.HecatonDataConnectionConfigLoader;
import uk.org.hecaton.dataconnection.impl.hecatondataconnection.HecatonDataConnectionConfigValidator;
import uk.org.hecaton.jet.impl.util.ConcurrentMemoizingSupplier;
import uk.org.hecaton.map.IMap;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Creates a HecatonInstance that is shared to connect to a remote cluster.
 * <p>
 * Set the client XML as property using {@link HecatonDataConnection#CLIENT_XML} as property key or
 * Set the client YAML as property using {@link HecatonDataConnection#CLIENT_YML} as property key
 *
 * @since 5.3
 */
public class HecatonDataConnection extends DataConnectionBase {

    /**
     * The constant to be used as property key for XML string for connecting to remote cluster
     */
    public static final String CLIENT_XML = "client_xml";

    /**
     * The constant to be used as property key for YAML string for connecting to remote cluster
     */
    public static final String CLIENT_YML = "client_yml";

    /**
     * The constant to be used as property key for XML file path for connecting to remote cluster
     */
    public static final String CLIENT_XML_PATH = "client_xml_path";

    /**
     * The constant to be used as property key for YAML file path for connecting to remote cluster
     */
    public static final String CLIENT_YML_PATH = "client_yml_path";

    /**
     * IMap Journal resource type name
     */
    public static final String OBJECT_TYPE_IMAP_JOURNAL = "IMapJournal";

    private final ClientConfig clientConfig;

    /**
     * The cached client instance
     */
    private ConcurrentMemoizingSupplier<HecatonClientProxy> proxy;

    public HecatonDataConnection(@Nonnull DataConnectionConfig dataConnectionConfig) {
        super(dataConnectionConfig);
        this.clientConfig = buildClientConfig();

        if (dataConnectionConfig.isShared()) {

            this.proxy = new ConcurrentMemoizingSupplier<>(() -> {
                HecatonClientProxy hecatonClientProxy = (HecatonClientProxy) HecatonClient
                        .newHecatonClient(clientConfig);
                return new HecatonClientProxy(hecatonClientProxy.client) {
                    @Override
                    public void shutdown() {
                        release();
                    }
                };
            });
        }
    }

    private ClientConfig buildClientConfig() {
        validateConfiguration();

        HecatonDataConnectionConfigLoader configLoader = new HecatonDataConnectionConfigLoader();
        DataConnectionConfig dataConnectionConfig = configLoader.load(getConfig());

        HecatonDataConnectionClientConfigBuilder configBuilder = new HecatonDataConnectionClientConfigBuilder();
        return configBuilder.buildClientConfig(dataConnectionConfig);
    }

    private void validateConfiguration() {
        HecatonDataConnectionConfigValidator validator = new HecatonDataConnectionConfigValidator();
        DataConnectionConfig dataConnectionConfig = getConfig();
        validator.validate(dataConnectionConfig);
    }

    @Nonnull
    @Override
    public Collection<String> resourceTypes() {
        return Collections.singleton(OBJECT_TYPE_IMAP_JOURNAL);
    }

    @Nonnull
    @Override
    public Collection<DataConnectionResource> listResources() {
        HecatonInstance instance = getClient();
        try {
            return instance.getDistributedObjects()
                    .stream()
                    .filter(IMap.class::isInstance)
                    .map(o -> new DataConnectionResource(OBJECT_TYPE_IMAP_JOURNAL, o.getName()))
                    .collect(Collectors.toList());
        } finally {
            instance.shutdown();
        }
    }

    /**
     * Return a client {@link HecatonInstance} based on this data connection configuration.
     * <p>
     * Depending on the {@link DataConnectionConfig#isShared()} config the HecatonInstance is
     * - shared=true -> a single instance is created
     * - shared=false -> a new instance is created each time the method is called
     * <p>
     * The caller must call `shutdown` on the instance when finished to allow correct
     * release of the underlying resources. In case of a shared instance, the instance
     * is shut down when all users call the shutdown method and this DataConnection is released.
     * In case of a non-shared instance, the instance is shutdown immediately
     * when the shutdown method is called.
     */
    @Nonnull
    public HecatonInstance getClient() {
        if (getConfig().isShared()) {
            retain();
            return proxy.get();
        } else {
            return HecatonClient.newHecatonClient(clientConfig);
        }
    }

    @Override
    public synchronized void destroy() {
        if (proxy != null) {
            HecatonClientProxy rememberedProxy = proxy.remembered();
            if (rememberedProxy != null) {
                // the proxy has overridden shutdown method, need to close real client
                rememberedProxy.client.shutdown();
            }
            proxy = null;
        }
    }
}
