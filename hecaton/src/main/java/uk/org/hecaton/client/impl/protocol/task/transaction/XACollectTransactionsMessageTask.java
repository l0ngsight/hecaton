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

package uk.org.hecaton.client.impl.protocol.task.transaction;

import uk.org.hecaton.client.impl.CollectRemoteTransactionsOperationSupplier;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.XATransactionCollectTransactionsCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractMultiTargetMessageTask;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.TransactionPermission;
import uk.org.hecaton.spi.impl.SerializableList;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.transaction.impl.xa.XAService;

import javax.transaction.xa.Xid;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class XACollectTransactionsMessageTask
        extends AbstractMultiTargetMessageTask<Void> {

    public XACollectTransactionsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Void decodeClientMessage(ClientMessage clientMessage) {
        return null;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return XATransactionCollectTransactionsCodec.encodeResponse((List<Xid>) response);
    }

    @Override
    protected Supplier<Operation> createOperationSupplier() {
        return new CollectRemoteTransactionsOperationSupplier();
    }

    @Override
    protected Object reduce(Map<Member, Object> map) throws Throwable {
        List<Data> list = new ArrayList<>();
        for (Object o : map.values()) {
            if (o instanceof Throwable throwable) {
                if (o instanceof MemberLeftException) {
                    continue;
                }
                throw throwable;
            }
            SerializableList xidSet = (SerializableList) o;
            list.addAll(xidSet.getCollection());
        }

        List<Xid> xidList = new ArrayList<>(list.size());
        list.forEach(xidData -> xidList.add(serializationService.toObject(xidData)));
        return xidList;
    }

    @Override
    public Collection<Member> getTargets() {
        return clientEngine.getClusterService().getMembers();
    }

    @Override
    public String getServiceName() {
        return XAService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.RECOVER;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }
}
