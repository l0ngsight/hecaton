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

package uk.org.hecaton.client.impl.protocol.task.list;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.ListRemoveWithIndexCodec;
import uk.org.hecaton.collection.impl.list.ListService;
import uk.org.hecaton.collection.impl.list.operations.ListRemoveOperation;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.ListPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link uk.org.hecaton.client.impl.protocol.codec.ListRemoveWithIndexCodec#REQUEST_MESSAGE_TYPE}
 */
public class ListRemoveWithIndexMessageTask
        extends AbstractListMessageTask<ListRemoveWithIndexCodec.RequestParameters> {

    public ListRemoveWithIndexMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new ListRemoveOperation(parameters.name, parameters.index);
    }

    @Override
    protected ListRemoveWithIndexCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ListRemoveWithIndexCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ListRemoveWithIndexCodec.encodeResponse((Data) response);
    }

    @Override
    public String getServiceName() {
        return ListService.SERVICE_NAME;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.index};
    }

    @Override
    public Permission getRequiredPermission() {
        return new ListPermission(parameters.name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.REMOVE;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

}
