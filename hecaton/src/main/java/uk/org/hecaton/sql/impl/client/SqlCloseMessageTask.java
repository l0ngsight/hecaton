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

package uk.org.hecaton.sql.impl.client;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.SqlCloseCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.sql.impl.InternalSqlService;
import uk.org.hecaton.sql.impl.QueryId;

import java.security.Permission;

/**
 * SQL query close task.
 */
public class SqlCloseMessageTask extends SqlAbstractMessageTask<QueryId> {
    public SqlCloseMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        InternalSqlService service = nodeEngine.getSqlService();

        service.getClientStateRegistry().close(endpoint.getUuid(), parameters);

        return null;
    }

    @Override
    protected QueryId decodeClientMessage(ClientMessage clientMessage) {
        return SqlCloseCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SqlCloseCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return InternalSqlService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "close";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters};
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
