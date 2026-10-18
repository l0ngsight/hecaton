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

package uk.org.hecaton.topic.impl;

import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.TopicConfig;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.internal.monitor.impl.LocalTopicStatsImpl;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.nio.ClassLoaderUtil;
import uk.org.hecaton.internal.util.ExceptionUtil;
import uk.org.hecaton.spi.impl.AbstractDistributedObject;
import uk.org.hecaton.spi.impl.InitializingObject;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.topic.LocalTopicStats;
import uk.org.hecaton.topic.MessageListener;

import javax.annotation.Nonnull;
import java.util.UUID;

public abstract class TopicProxySupport extends AbstractDistributedObject<TopicService> implements InitializingObject {

    private final String name;
    private final TopicService topicService;
    private final LocalTopicStatsImpl topicStats;
    private final OperationService operationService;
    private final int partitionId;
    private boolean multithreaded;
    private ClassLoader classLoader;

    protected TopicProxySupport(String name, NodeEngine nodeEngine, TopicService service) {
        super(nodeEngine, service);
        this.name = name;
        this.topicService = service;
        this.topicStats = topicService.getLocalTopicStats(name);
        this.operationService = nodeEngine.getOperationService();
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    @Override
    public void initialize() {
        NodeEngine nodeEngine = getNodeEngine();
        TopicConfig config = nodeEngine.getConfig().findTopicConfig(name);
        multithreaded = config.isMultiThreadingEnabled();
        classLoader = NamespaceUtil.getClassLoaderForNamespace(nodeEngine, config.getUserCodeNamespace());
        for (ListenerConfig listenerConfig : config.getMessageListenerConfigs()) {
            initialize(listenerConfig);
        }
    }

    private void initialize(ListenerConfig listenerConfig) {
        MessageListener listener = loadListener(listenerConfig);
        if (listener == null) {
            return;
        }

        addMessageListenerInternal(listener);
    }

    private MessageListener loadListener(ListenerConfig listenerConfig) {
        try {
            MessageListener listener = (MessageListener) listenerConfig.getImplementation();
            if (listener == null && listenerConfig.getClassName() != null) {
                listener = ClassLoaderUtil.newInstance(classLoader, listenerConfig.getClassName());
            }
            return listener;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public LocalTopicStats getLocalTopicStatsInternal() {
        return topicService.getLocalTopicStats(name);
    }

    /**
     * Publishes the message and increases the local statistics
     * for the number of published messages.
     *
     * @param message the message to be published
     */
    public void publishInternal(@Nonnull Object message) {
        topicService.publishMessage(name, message, multithreaded);
        topicStats.incrementPublishes();
    }

    public @Nonnull
    UUID addMessageListenerInternal(@Nonnull MessageListener listener) {
        if (listener instanceof HecatonInstanceAware aware) {
            aware.setHecatonInstance(getNodeEngine().getHecatonInstance());
        }
        return topicService.addMessageListener(name, listener);
    }

    public boolean removeMessageListenerInternal(@Nonnull UUID registrationId) {
        return topicService.removeMessageListener(name, registrationId);
    }

    @Override
    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }
}
