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

package uk.org.hecaton.map.impl.operation;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.namespace.impl.NodeEngineThreadLocalContext;
import uk.org.hecaton.internal.serialization.impl.SerializationUtil;
import uk.org.hecaton.map.IMapEvent;
import uk.org.hecaton.map.MapInterceptor;
import uk.org.hecaton.map.impl.InterceptorRegistry;
import uk.org.hecaton.map.impl.ListenerAdapter;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapDataSerializerHook;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.querycache.QueryCacheContext;
import uk.org.hecaton.map.impl.querycache.accumulator.AccumulatorInfo;
import uk.org.hecaton.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import uk.org.hecaton.map.impl.querycache.publisher.MapPublisherRegistry;
import uk.org.hecaton.map.impl.querycache.publisher.PublisherContext;
import uk.org.hecaton.map.impl.querycache.publisher.PublisherRegistry;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.TargetAware;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static uk.org.hecaton.internal.util.MapUtil.createHashMap;

public class PostJoinMapOperation extends Operation implements IdentifiedDataSerializable, TargetAware {

    private List<InterceptorInfo> interceptorInfoList = new LinkedList<>();
    private List<AccumulatorInfo> infoList;

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public void addMapInterceptors(MapContainer mapContainer) {
        InterceptorRegistry interceptorRegistry = mapContainer.getInterceptorRegistry();
        List<MapInterceptor> interceptorList = interceptorRegistry.getInterceptors();
        // we do not send info for maps that do not have interceptors
        if (interceptorList.isEmpty()) {
            return;
        }
        Map<String, MapInterceptor> interceptorMap = interceptorRegistry.getId2InterceptorMap();
        Map<MapInterceptor, String> revMap = createHashMap(interceptorMap.size());
        for (Map.Entry<String, MapInterceptor> entry : interceptorMap.entrySet()) {
            revMap.put(entry.getValue(), entry.getKey());
        }
        InterceptorInfo interceptorInfo = new InterceptorInfo(mapContainer.getName());
        for (MapInterceptor interceptor : interceptorList) {
            interceptorInfo.addInterceptor(revMap.get(interceptor), interceptor);
        }
        interceptorInfoList.add(interceptorInfo);
    }

    public static class InterceptorInfo implements IdentifiedDataSerializable {

        private String mapName;
        private final List<Map.Entry<String, MapInterceptor>> interceptors = new LinkedList<>();

        InterceptorInfo(String mapName) {
            this.mapName = mapName;
        }

        public InterceptorInfo() {
        }

        void addInterceptor(String id, MapInterceptor interceptor) {
            interceptors.add(new AbstractMap.SimpleImmutableEntry<>(id, interceptor));
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(mapName);
            out.writeInt(interceptors.size());
            for (Map.Entry<String, MapInterceptor> entry : interceptors) {
                out.writeString(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            mapName = in.readString();
            int size = in.readInt();

            NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
            String namespace = MapService.lookupNamespace(engine, mapName);
            NamespaceUtil.setupNamespace(engine, namespace);
            try {
                for (int i = 0; i < size; i++) {
                    String id = in.readString();
                    MapInterceptor interceptor = in.readObject();
                    interceptors.add(new AbstractMap.SimpleImmutableEntry<>(id, interceptor));
                }
            } finally {
                NamespaceUtil.cleanupNamespace(engine, namespace);
            }
        }

        @Override
        public int getFactoryId() {
            return MapDataSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return MapDataSerializerHook.INTERCEPTOR_INFO;
        }
    }

    @Override
    public void run() throws Exception {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        for (InterceptorInfo interceptorInfo : interceptorInfoList) {
            final MapContainer mapContainer = mapServiceContext.getMapContainer(interceptorInfo.mapName);
            InterceptorRegistry interceptorRegistry = mapContainer.getInterceptorRegistry();
            Map<String, MapInterceptor> interceptorMap = interceptorRegistry.getId2InterceptorMap();
            List<Map.Entry<String, MapInterceptor>> entryList = interceptorInfo.interceptors;
            for (Map.Entry<String, MapInterceptor> entry : entryList) {
                if (!interceptorMap.containsKey(entry.getKey())) {
                    interceptorRegistry.register(entry.getKey(), entry.getValue());
                }
            }
        }
        createQueryCaches();
    }

    private void createQueryCaches() {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();

        for (AccumulatorInfo info : infoList) {
            addAccumulatorInfo(queryCacheContext, info);

            PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrCreate(info.getMapName());
            publisherRegistry.getOrCreate(info.getCacheId());
            // marker listener.
            mapServiceContext.addLocalListenerAdapter((ListenerAdapter<IMapEvent>) event -> {

            }, info.getMapName());
        }
    }

    private void addAccumulatorInfo(QueryCacheContext context, AccumulatorInfo info) {
        PublisherContext publisherContext = context.getPublisherContext();
        AccumulatorInfoSupplier infoSupplier = publisherContext.getAccumulatorInfoSupplier();
        infoSupplier.putIfAbsent(info.getMapName(), info.getCacheId(), info);
    }

    public void setInfoList(List<AccumulatorInfo> infoList) {
        this.infoList = infoList;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        SerializationUtil.writeList(interceptorInfoList, out);
        SerializationUtil.writeList(infoList, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        int interceptorsCount = in.readInt();
        for (int i = 0; i < interceptorsCount; i++) {
            interceptorInfoList.add(in.readObject());
        }
        infoList = SerializationUtil.readList(in);
    }


    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.POST_JOIN_MAP_OPERATION;
    }

    @Override
    public void setTarget(Address address) {
    }
}
