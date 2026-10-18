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

package uk.org.hecaton.client.impl.protocol.task.multimap;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapContainsValueCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.monitor.impl.LocalMapStatsImpl;
import uk.org.hecaton.multimap.impl.operations.MultiMapOperationFactory;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MultiMapPermission;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Map;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link uk.org.hecaton.client.impl.protocol.codec.MultiMapContainsValueCodec#REQUEST_MESSAGE_TYPE}
 */
public class MultiMapContainsValueMessageTask
        extends AbstractMultiMapAllPartitionsMessageTask<MultiMapContainsValueCodec.RequestParameters> {

    public MultiMapContainsValueMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new MultiMapOperationFactory(parameters.name,
                MultiMapOperationFactory.OperationFactoryType.CONTAINS, null, parameters.value);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        boolean found = false;
        for (Object obj : map.values()) {
            if (Boolean.TRUE.equals(obj)) {
                found = true;
                break;
            }
        }
        updateStats(LocalMapStatsImpl::incrementOtherOperations);
        return found;
    }

    @Override
    protected MultiMapContainsValueCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapContainsValueCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MultiMapContainsValueCodec.encodeResponse((Boolean) response);
    }

    @Override
    public Permission getRequiredPermission() {
        return new MultiMapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.CONTAINS_VALUE;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.value};
    }

}
