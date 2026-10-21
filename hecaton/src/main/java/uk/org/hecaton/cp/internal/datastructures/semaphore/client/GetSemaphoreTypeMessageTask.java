/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.cp.internal.datastructures.semaphore.client;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.SemaphoreGetSemaphoreTypeCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractMessageTask;
import uk.org.hecaton.config.cp.SemaphoreConfig;
import uk.org.hecaton.cp.internal.datastructures.semaphore.SemaphoreService;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.SemaphorePermission;

import java.security.Permission;

/**
 * Client message task for querying semaphore JDK compatibility
 */
public class GetSemaphoreTypeMessageTask extends AbstractMessageTask<String> {

    public GetSemaphoreTypeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        SemaphoreConfig config = nodeEngine.getConfig().getCPSubsystemConfig().findSemaphoreConfig(parameters);
        boolean jdkCompatible = (config != null && config.isJDKCompatible());
        sendResponse(jdkCompatible);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return SemaphoreGetSemaphoreTypeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SemaphoreGetSemaphoreTypeCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new SemaphorePermission(parameters);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

}
