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

package uk.org.hecaton.internal.jmx;

import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.collection.impl.list.ListService;
import uk.org.hecaton.collection.impl.queue.QueueService;
import uk.org.hecaton.collection.impl.set.SetService;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.executor.impl.DistributedExecutorService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.multimap.MultiMap;
import uk.org.hecaton.multimap.impl.MultiMapService;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapProxy;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.topic.ITopic;
import uk.org.hecaton.topic.impl.TopicService;
import uk.org.hecaton.topic.impl.reliable.ReliableTopicProxy;
import uk.org.hecaton.topic.impl.reliable.ReliableTopicService;

import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.internal.util.MapUtil.createConcurrentHashMap;

/**
 * A helper class which contains various types of {@link HecatonMBean} factory methods and metadata.
 */
final class MBeans {

    private static final ConcurrentMap<String, MBeanFactory> MBEAN_FACTORY_TYPES_REGISTRY
            = createConcurrentHashMap(MBeanFactory.values().length);

    static {
        MBeanFactory[] mBeanFactories = MBeanFactory.values();
        for (MBeanFactory mBeanFactory : mBeanFactories) {
            MBEAN_FACTORY_TYPES_REGISTRY.put(mBeanFactory.getServiceName(), mBeanFactory);
        }
    }

    private MBeans() {
    }

    static HecatonMBean createHecatonMBeanOrNull(DistributedObject distributedObject,
                                                     ManagementService managementService) {
        MBeanFactory mBeanFactory = getMBeanFactory(distributedObject.getServiceName());
        return mBeanFactory == null ? null : mBeanFactory.createNew(distributedObject, managementService);
    }

    static String getObjectTypeOrNull(String serviceName) {
        MBeanFactory mBeanFactory = getMBeanFactory(serviceName);
        return mBeanFactory == null ? null : mBeanFactory.getObjectType();
    }

    private static MBeanFactory getMBeanFactory(String serviceName) {
        return MBEAN_FACTORY_TYPES_REGISTRY.get(serviceName);
    }

    enum MBeanFactory {

        MAP {
            @Override
            public HecatonMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new MapMBean((IMap) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "IMap";
            }

            @Override
            public String getServiceName() {
                return MapService.SERVICE_NAME;
            }
        },

        LIST {
            @Override
            public HecatonMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new ListMBean((IList) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "IList";
            }

            @Override
            public String getServiceName() {
                return ListService.SERVICE_NAME;
            }
        },

        MULTI_MAP {
            @Override
            public HecatonMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new MultiMapMBean((MultiMap) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "MultiMap";
            }

            @Override
            public String getServiceName() {
                return MultiMapService.SERVICE_NAME;
            }
        },

        QUEUE {
            @Override
            public HecatonMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new QueueMBean((IQueue) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "IQueue";
            }

            @Override
            public String getServiceName() {
                return QueueService.SERVICE_NAME;
            }
        },

        EXECUTOR_SERVICE {
            @Override
            public HecatonMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new ExecutorServiceMBean((IExecutorService) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "IExecutorService";
            }

            @Override
            public String getServiceName() {
                return DistributedExecutorService.SERVICE_NAME;
            }
        },

        SET {
            @Override
            public HecatonMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new SetMBean((ISet) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "ISet";
            }

            @Override
            public String getServiceName() {
                return SetService.SERVICE_NAME;
            }
        },

        TOPIC {
            @Override
            public HecatonMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new TopicMBean((ITopic) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "ITopic";
            }

            @Override
            public String getServiceName() {
                return TopicService.SERVICE_NAME;
            }
        },

        REPLICATED_MAP {
            @Override
            public HecatonMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new ReplicatedMapMBean((ReplicatedMapProxy) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "ReplicatedMap";
            }

            @Override
            public String getServiceName() {
                return ReplicatedMapService.SERVICE_NAME;
            }
        },

        RELIABLE_TOPIC {
            @Override
            public HecatonMBean createNew(DistributedObject distributedObject, ManagementService managementService) {
                return new ReliableTopicMBean((ReliableTopicProxy) distributedObject, managementService);
            }

            @Override
            public String getObjectType() {
                return "ReliableTopic";
            }

            @Override
            public String getServiceName() {
                return ReliableTopicService.SERVICE_NAME;
            }
        };

        abstract HecatonMBean createNew(DistributedObject distributedObject, ManagementService managementService);

        abstract String getObjectType();

        abstract String getServiceName();
    }
}
