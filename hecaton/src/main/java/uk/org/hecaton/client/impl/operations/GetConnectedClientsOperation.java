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

package uk.org.hecaton.client.impl.operations;

import uk.org.hecaton.client.Client;
import uk.org.hecaton.client.impl.ClientDataSerializerHook;
import uk.org.hecaton.client.impl.ClientEndpointImpl;
import uk.org.hecaton.client.impl.ClientEngine;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static uk.org.hecaton.internal.util.MapUtil.createHashMap;


public class GetConnectedClientsOperation extends AbstractClientOperation implements ReadonlyOperation {

    private Map<UUID, String> clients;

    public GetConnectedClientsOperation() {
    }

    @Override
    public void run() throws Exception {
        ClientEngine service = getService();
        final Collection<Client> serviceClients = service.getClients();
        this.clients = createHashMap(serviceClients.size());
        for (Client clientEndpoint : serviceClients) {
            ClientEndpointImpl clientEndpointImpl = (ClientEndpointImpl) clientEndpoint;
            this.clients.put(clientEndpointImpl.getUuid(), clientEndpointImpl.getClientType());
        }
    }

    @Override
    public Object getResponse() {
        return clients;
    }

    @Override
    public int getClassId() {
        return ClientDataSerializerHook.GET_CONNECTED_CLIENTS;
    }

}
