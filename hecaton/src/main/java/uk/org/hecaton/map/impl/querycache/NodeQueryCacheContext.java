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

package uk.org.hecaton.map.impl.querycache;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.instance.impl.LifecycleServiceImpl;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.util.ContextMutexFactory;
import uk.org.hecaton.map.IMapEvent;
import uk.org.hecaton.map.impl.ListenerAdapter;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.querycache.publisher.DefaultPublisherContext;
import uk.org.hecaton.map.impl.querycache.publisher.PublisherContext;
import uk.org.hecaton.map.impl.querycache.subscriber.NodeQueryCacheConfigurator;
import uk.org.hecaton.map.impl.querycache.subscriber.NodeQueryCacheEventService;
import uk.org.hecaton.map.impl.querycache.subscriber.NodeQueryCacheScheduler;
import uk.org.hecaton.map.impl.querycache.subscriber.NodeSubscriberContext;
import uk.org.hecaton.map.impl.querycache.subscriber.SubscriberContext;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.eventservice.EventService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;

/**
 * Node side implementation of {@link QueryCacheContext}.
 *
 * @see QueryCacheContext
 */
public class NodeQueryCacheContext implements QueryCacheContext {

    private final NodeEngine nodeEngine;
    private final InvokerWrapper invokerWrapper;
    private final MapServiceContext mapServiceContext;
    private final QueryCacheScheduler queryCacheScheduler;
    private final QueryCacheEventService queryCacheEventService;
    private final QueryCacheConfigurator queryCacheConfigurator;
    private final ContextMutexFactory lifecycleMutexFactory = new ContextMutexFactory();

    // these fields are not final for testing purposes
    private PublisherContext publisherContext;
    private SubscriberContext subscriberContext;

    public NodeQueryCacheContext(MapServiceContext mapServiceContext) {
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.mapServiceContext = mapServiceContext;
        this.queryCacheScheduler = new NodeQueryCacheScheduler(mapServiceContext);
        this.queryCacheEventService = new NodeQueryCacheEventService(mapServiceContext, lifecycleMutexFactory);
        this.queryCacheConfigurator = new NodeQueryCacheConfigurator(nodeEngine, nodeEngine.getConfig(), queryCacheEventService);
        this.invokerWrapper = new NodeInvokerWrapper(nodeEngine.getOperationService());
        // init these in the end
        this.subscriberContext = new NodeSubscriberContext(this);
        this.publisherContext = new DefaultPublisherContext(this, nodeEngine, new RegisterMapListenerFunction());
        flushPublishersOnNodeShutdown();
    }

    /**
     * This is a best effort approach; there is no guarantee that events in publishers internal buffers will be fired,
     * {@link EventService} can drop them.
     */
    private void flushPublishersOnNodeShutdown() {
        Node node = this.nodeEngine.getNode();
        LifecycleServiceImpl lifecycleService = node.hecatonInstance.getLifecycleService();
        lifecycleService.addLifecycleListener(event -> {
            if (SHUTTING_DOWN == event.getState()) {
                publisherContext.flush();
            }
        });
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PublisherContext getPublisherContext() {
        return publisherContext;
    }

    @Override
    public SubscriberContext getSubscriberContext() {
        return subscriberContext;
    }

    @Override
    public void setSubscriberContext(SubscriberContext subscriberContext) {
        this.subscriberContext = subscriberContext;
    }

    @Override
    public QueryCacheEventService getQueryCacheEventService() {
        return queryCacheEventService;
    }

    @Override
    public QueryCacheConfigurator getQueryCacheConfigurator() {
        return queryCacheConfigurator;
    }

    @Override
    public QueryCacheScheduler getQueryCacheScheduler() {
        return queryCacheScheduler;
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return (InternalSerializationService) nodeEngine.getSerializationService();
    }

    @Override
    public Address getThisNodesAddress() {
        return nodeEngine.getThisAddress();
    }

    @Override
    public Collection<Member> getMemberList() {
        Collection<MemberImpl> memberList = nodeEngine.getClusterService().getMemberImpls();
        List<Member> members = new ArrayList<>(memberList.size());
        members.addAll(memberList);
        return members;
    }

    @Override
    public int getPartitionId(Object object) {
        assert object != null;

        if (object instanceof Data data) {
            nodeEngine.getPartitionService().getPartitionId(data);
        }
        return nodeEngine.getPartitionService().getPartitionId(object);
    }

    @Override
    public int getPartitionCount() {
        return nodeEngine.getPartitionService().getPartitionCount();
    }

    @Override
    public InvokerWrapper getInvokerWrapper() {
        return invokerWrapper;
    }

    @Override
    public Object toObject(Object obj) {
        return mapServiceContext.toObject(obj);
    }

    @Override
    public ContextMutexFactory getLifecycleMutexFactory() {
        return lifecycleMutexFactory;
    }

    private UUID registerLocalIMapListener(final String name) {
        return mapServiceContext.addLocalListenerAdapter(new ListenerAdapter<IMapEvent>() {
            @Override
            public void onEvent(IMapEvent event) {
                // NOP
            }

            @Override
            public String toString() {
                return "Local IMap listener for the map '" + name + "'";
            }
        }, name);
    }

    private class RegisterMapListenerFunction implements Function<String, UUID> {
        @Override
        public UUID apply(String name) {
            return registerLocalIMapListener(name);
        }
    }
}
