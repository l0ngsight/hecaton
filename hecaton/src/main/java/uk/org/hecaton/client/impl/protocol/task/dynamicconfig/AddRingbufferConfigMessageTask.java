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

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddRingbufferConfigCodec;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MergePolicyConfig;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.config.RingbufferStoreConfig;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;

import java.security.Permission;

public class AddRingbufferConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddRingbufferConfigCodec.RequestParameters> {

    public AddRingbufferConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddRingbufferConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddRingbufferConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddRingbufferConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        RingbufferConfig config = new RingbufferConfig(parameters.name);
        config.setAsyncBackupCount(parameters.asyncBackupCount);
        config.setBackupCount(parameters.backupCount);
        config.setCapacity(parameters.capacity);
        config.setInMemoryFormat(InMemoryFormat.valueOf(parameters.inMemoryFormat));
        config.setTimeToLiveSeconds(parameters.timeToLiveSeconds);
        if (parameters.ringbufferStoreConfig != null) {
            RingbufferStoreConfig storeConfig = NamespaceUtil.callWithNamespace(nodeEngine, parameters.userCodeNamespace,
                    () -> parameters.ringbufferStoreConfig.asRingbufferStoreConfig(serializationService));
            config.setRingbufferStoreConfig(storeConfig);
        }
        MergePolicyConfig mergePolicyConfig = mergePolicyConfig(parameters.mergePolicy, parameters.mergeBatchSize);
        config.setMergePolicyConfig(mergePolicyConfig);
        if (parameters.isUserCodeNamespaceExists) {
            config.setUserCodeNamespace(parameters.userCodeNamespace);
        }
        return config;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_RINGBUFFER_CONFIG;
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        return parameters.userCodeNamespace != null
                ? new UserCodeNamespacePermission(parameters.userCodeNamespace, ActionConstants.ACTION_USE) : null;
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        DynamicConfigurationAwareConfig nodeConfig = (DynamicConfigurationAwareConfig) nodeEngine.getConfig();
        RingbufferConfig ringbufferConfig = (RingbufferConfig) config;
        return DynamicConfigurationAwareConfig.checkStaticConfigDoesNotExist(nodeConfig.getStaticConfig().getRingbufferConfigs(),
                ringbufferConfig.getName(), ringbufferConfig);
    }
}
