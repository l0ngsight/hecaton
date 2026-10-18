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
import uk.org.hecaton.config.Config;
import uk.org.hecaton.instance.BuildInfo;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeExtension;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.dynamicconfig.ClusterWideConfigurationService;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.NodeEngine;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;

/**
 * Base implementation for dynamic add***Config methods.
 */
public abstract class AbstractAddConfigMessageTask<P> extends AbstractUpdateConfigMessageTask<P> {
    protected AbstractAddConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    protected AbstractAddConfigMessageTask(ClientMessage clientMessage, ILogger logger, NodeEngine nodeEngine,
            InternalSerializationService serializationService, ClientEngine clientEngine, Connection connection,
            NodeExtension nodeExtension, BuildInfo buildInfo, Config config, ClusterServiceImpl clusterService) {
        super(clientMessage, logger, nodeEngine, serializationService, clientEngine, connection, nodeExtension, buildInfo,
                config, clusterService);
    }

    protected abstract boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config);

    @Override
    public final void processMessage() {
        IdentifiedDataSerializable config = getConfig();
        ClusterWideConfigurationService service = getService(getServiceName());
        if (checkStaticConfigDoesNotExist(config)) {
            service.broadcastConfigAsync(config).whenCompleteAsync(this, CALLER_RUNS);
        } else {
            sendResponse(null);
        }
    }
}
