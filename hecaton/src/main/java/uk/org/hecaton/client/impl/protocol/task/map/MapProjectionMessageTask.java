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

package uk.org.hecaton.client.impl.protocol.task.map;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MapProjectCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapProjectWithPredicateCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.projection.Projection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import java.util.List;

public class MapProjectionMessageTask
        extends DefaultMapProjectMessageTask<MapProjectCodec.RequestParameters> {

    public MapProjectionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Projection<?, ?> getProjection() {
        return nodeEngine.getSerializationService().toObject(parameters.projection);
    }

    @Override
    protected MapProjectCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapProjectCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapProjectWithPredicateCodec.encodeResponse((List<Data>) response);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.PROJECT;
    }

    @Override
    public Object[] getParameters() {
        return new Object[] { parameters.name, parameters.projection };
    }

}
