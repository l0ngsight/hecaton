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

package uk.org.hecaton.client.impl.protocol.task;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.ClientCreateProxyCodec;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.memberselector.MemberSelectors;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.spi.impl.operationservice.InvocationBuilder;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.impl.proxyservice.ProxyService;
import uk.org.hecaton.spi.impl.proxyservice.impl.operations.InitializeDistributedObjectOperation;

import java.security.Permission;
import java.util.Collection;
import java.util.Iterator;

public class CreateProxyMessageTask extends AbstractInvocationMessageTask<ClientCreateProxyCodec.RequestParameters>
        implements BlockingMessageTask {

    public CreateProxyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        final OperationService operationService = nodeEngine.getOperationService();
        if (!nodeEngine.getLocalMember().isLiteMember()) {
            //if this is a data member run the create proxy here
            return operationService.createInvocationBuilder(getServiceName(), op, nodeEngine.getThisAddress());
        }
        //otherwise find a data member to send the operation
        Collection<Member> members = nodeEngine.getClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);
        Iterator<Member> iterator = members.iterator();
        if (iterator.hasNext()) {
            Member member = iterator.next();
            Address address = member.getAddress();
            return operationService.createInvocationBuilder(getServiceName(), op, address);
        }
        //if no data member found run on this member. CreateProxy operation itself will send exception if not able to create.
        return operationService.createInvocationBuilder(getServiceName(), op, nodeEngine.getThisAddress());
    }

    @Override
    protected Operation prepareOperation() {
        return new InitializeDistributedObjectOperation(parameters.serviceName, parameters.name);
    }

    @Override
    protected ClientCreateProxyCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ClientCreateProxyCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientCreateProxyCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return parameters.serviceName;
    }

    @Override
    public Permission getRequiredPermission() {
        ProxyService proxyService = clientEngine.getProxyService();
        if (proxyService.existsDistributedObject(parameters.serviceName, parameters.name)) {
            return null;
        }
        return ActionConstants.getPermission(parameters.name, parameters.serviceName, ActionConstants.ACTION_CREATE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.CREATE;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.name};
    }
}
