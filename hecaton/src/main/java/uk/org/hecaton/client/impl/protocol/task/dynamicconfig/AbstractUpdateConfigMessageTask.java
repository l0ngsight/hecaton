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

package uk.org.hecaton.client.impl.protocol.task.dynamicconfig;

import uk.org.hecaton.client.impl.ClientEngine;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.task.AbstractMessageTask;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.MergePolicyConfig;
import uk.org.hecaton.instance.BuildInfo;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeExtension;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.dynamicconfig.ConfigurationService;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.security.permission.ConfigPermission;
import uk.org.hecaton.spi.impl.NodeEngine;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public abstract class AbstractUpdateConfigMessageTask<P> extends AbstractMessageTask<P>
        implements BiConsumer<Object, Throwable> {

    private static final ConfigPermission CONFIG_PERMISSION = new ConfigPermission();

    protected AbstractUpdateConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    protected AbstractUpdateConfigMessageTask(ClientMessage clientMessage, ILogger logger, NodeEngine nodeEngine,
            InternalSerializationService serializationService, ClientEngine clientEngine, Connection connection,
            NodeExtension nodeExtension, BuildInfo buildInfo, Config config, ClusterServiceImpl clusterService) {
        super(clientMessage, logger, nodeEngine, serializationService, clientEngine, connection, nodeExtension, buildInfo,
                config, clusterService);
    }

    @Override
    public String getServiceName() {
        return ConfigurationService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return CONFIG_PERMISSION;
    }

    @Override
    public Object[] getParameters() {
        // todo: may have to specify for security
        return new Object[0];
    }

    @Override
    public void accept(Object response, Throwable throwable) {
        if (throwable == null) {
            sendResponse(response);
        } else {
            handleProcessingFailure(throwable);
        }
    }

    protected MergePolicyConfig mergePolicyConfig(String mergePolicy, int batchSize) {
        return new MergePolicyConfig(mergePolicy, batchSize);
    }

    protected List<? extends ListenerConfig> adaptListenerConfigs(List<ListenerConfigHolder> listenerConfigHolders,
                                                                  String namespace) {
        if (listenerConfigHolders == null || listenerConfigHolders.isEmpty()) {
            return null;
        } else {
            return listenerConfigHolders.stream()
                    .<ListenerConfig>map(listenerConfigHolder ->
                            listenerConfigHolder.asListenerConfig(serializationService, namespace))
                    .collect(Collectors.toCollection(ArrayList::new));
        }
    }

    protected abstract IdentifiedDataSerializable getConfig();
}
