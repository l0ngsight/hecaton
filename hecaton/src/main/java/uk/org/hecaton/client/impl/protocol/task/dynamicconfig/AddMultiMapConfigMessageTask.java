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
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddMultiMapConfigCodec;
import uk.org.hecaton.config.EntryListenerConfig;
import uk.org.hecaton.config.MergePolicyConfig;
import uk.org.hecaton.config.MultiMapConfig;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;

import java.security.Permission;

public class AddMultiMapConfigMessageTask extends
        AbstractAddConfigMessageTask<DynamicConfigAddMultiMapConfigCodec.RequestParameters> {

    public AddMultiMapConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddMultiMapConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddMultiMapConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddMultiMapConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        MultiMapConfig multiMapConfig = new MultiMapConfig();
        multiMapConfig.setName(parameters.name);
        multiMapConfig.setValueCollectionType(parameters.collectionType);
        multiMapConfig.setAsyncBackupCount(parameters.asyncBackupCount);
        multiMapConfig.setBackupCount(parameters.backupCount);
        multiMapConfig.setBinary(parameters.binary);
        multiMapConfig.setStatisticsEnabled(parameters.statisticsEnabled);
        if (parameters.listenerConfigs != null && !parameters.listenerConfigs.isEmpty()) {
            for (ListenerConfigHolder configHolder : parameters.listenerConfigs) {
                EntryListenerConfig entryListenerConfig =
                        configHolder.asListenerConfig(serializationService, parameters.userCodeNamespace);
                multiMapConfig.addEntryListenerConfig(entryListenerConfig);
            }
        }
        MergePolicyConfig mergePolicyConfig = mergePolicyConfig(parameters.mergePolicy, parameters.mergeBatchSize);
        multiMapConfig.setMergePolicyConfig(mergePolicyConfig);
        if (parameters.isUserCodeNamespaceExists) {
            multiMapConfig.setUserCodeNamespace(parameters.userCodeNamespace);
        }
        return multiMapConfig;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_MULTIMAP_CONFIG;
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        return parameters.userCodeNamespace != null
                ? new UserCodeNamespacePermission(parameters.userCodeNamespace, ActionConstants.ACTION_USE) : null;
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        DynamicConfigurationAwareConfig nodeConfig = (DynamicConfigurationAwareConfig) nodeEngine.getConfig();
        MultiMapConfig multiMapConfig = (MultiMapConfig) config;
        return DynamicConfigurationAwareConfig.checkStaticConfigDoesNotExist(nodeConfig.getStaticConfig().getMultiMapConfigs(),
                multiMapConfig.getName(), multiMapConfig);
    }
}
