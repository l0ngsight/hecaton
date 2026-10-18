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

package uk.org.hecaton.client.impl.protocol.task.set;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.SetContainsCodec;
import uk.org.hecaton.collection.impl.collection.operations.CollectionContainsOperation;
import uk.org.hecaton.collection.impl.set.SetService;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.SetPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;

import static java.util.Collections.singleton;

/**
 * SetContainsMessageTask
 */
public class SetContainsMessageTask
        extends AbstractSetMessageTask<SetContainsCodec.RequestParameters> {

    public SetContainsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new CollectionContainsOperation(parameters.name, singleton(parameters.value));
    }

    @Override
    protected SetContainsCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SetContainsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SetContainsCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return SetService.SERVICE_NAME;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.value};
    }

    @Override
    public Permission getRequiredPermission() {
        return new SetPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.CONTAINS;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

}
