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

package uk.org.hecaton.client.impl;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.DefaultMessageTaskFactoryProvider;
import uk.org.hecaton.client.impl.protocol.MessageTaskFactory;
import uk.org.hecaton.client.impl.protocol.MessageTaskFactoryProvider;
import uk.org.hecaton.client.impl.protocol.task.MessageTask;
import uk.org.hecaton.client.impl.protocol.task.NoSuchMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.util.ExceptionUtil;
import uk.org.hecaton.internal.util.ServiceLoader;
import uk.org.hecaton.internal.util.collection.Int2ObjectHashMap;
import uk.org.hecaton.spi.impl.NodeEngine;

import java.lang.reflect.Constructor;
import java.util.Iterator;

public class CompositeMessageTaskFactory implements MessageTaskFactory {
    private static final String FACTORY_ID = "uk.org.hecaton.client.impl.protocol.MessageTaskFactoryProvider";

    private final Node node;
    private final NodeEngine nodeEngine;
    private final Int2ObjectHashMap<MessageTaskFactory> factories;

    public CompositeMessageTaskFactory(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        MessageTaskFactoryProvider defaultProvider = new DefaultMessageTaskFactoryProvider(this.nodeEngine);
        this.factories = new Int2ObjectHashMap<>(defaultProvider.getFactories().size());
        loadProvider(defaultProvider);
        loadServices();
    }

    private void loadProvider(MessageTaskFactoryProvider provider) {
        Int2ObjectHashMap<MessageTaskFactory> providerFactories = provider.getFactories();
        this.factories.putAll(providerFactories);
    }

    private void loadServices() {
        try {
            ClassLoader classLoader = this.node.getConfigClassLoader();
            Iterator<Class<MessageTaskFactoryProvider>> iter = ServiceLoader.classIterator(
                    MessageTaskFactoryProvider.class, FACTORY_ID, classLoader);

            while (iter.hasNext()) {
                Class<MessageTaskFactoryProvider> clazz = iter.next();
                Constructor<MessageTaskFactoryProvider> constructor = clazz
                        .getDeclaredConstructor(new Class[]{NodeEngine.class});
                MessageTaskFactoryProvider messageTaskProvider = constructor.newInstance(this.nodeEngine);
                loadProvider(messageTaskProvider);
            }
        } catch (Throwable e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public MessageTask create(ClientMessage clientMessage, Connection connection) {
        try {
            final MessageTaskFactory factory = this.factories.get(clientMessage.getMessageType());
            if (factory != null) {
                return factory.create(clientMessage, connection);
            }
        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        }
        return new NoSuchMessageTask(clientMessage, this.node, connection);
    }
}
