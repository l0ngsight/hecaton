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

package uk.org.hecaton.jet.impl.connector;

import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.function.BiConsumerEx;
import uk.org.hecaton.function.BiFunctionEx;
import uk.org.hecaton.function.BinaryOperatorEx;
import uk.org.hecaton.function.ConsumerEx;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.jet.RestartableException;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorMetaSupplier;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.core.processor.SinkProcessors;
import uk.org.hecaton.jet.impl.observer.ObservableImpl;
import uk.org.hecaton.jet.impl.util.ImdgUtil;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.security.impl.function.SecuredFunctions;
import uk.org.hecaton.security.permission.RingBufferPermission;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serial;
import java.security.Permission;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import static uk.org.hecaton.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static uk.org.hecaton.jet.impl.connector.AsyncHecatonWriterP.MAX_PARALLEL_ASYNC_OPS_DEFAULT;
import static uk.org.hecaton.jet.impl.util.ImdgUtil.asXmlString;
import static uk.org.hecaton.jet.impl.util.Util.checkSerializable;
import static uk.org.hecaton.security.PermissionsUtil.cachePutPermission;
import static uk.org.hecaton.security.PermissionsUtil.listAddPermission;
import static uk.org.hecaton.security.PermissionsUtil.mapPutPermission;
import static uk.org.hecaton.security.PermissionsUtil.mapUpdatePermission;
import static uk.org.hecaton.security.permission.ActionConstants.ACTION_CREATE;
import static uk.org.hecaton.security.permission.ActionConstants.ACTION_PUT;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/**
 * This is private API. Check out the {@link SinkProcessors} class for
 * public factory methods.
 */
public final class HecatonWriters {

    private HecatonWriters() {
    }

    /**
     * Update map with key and value functions
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier writeMapSupplier(
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> toValueFn
    ) {
        MapSinkConfiguration<T, K, V> params = new MapSinkConfiguration<>(name);
        params.setClientXml(ImdgUtil.asXmlString(clientConfig));
        params.setToKeyFn(toKeyFn);
        params.setToValueFn(toValueFn);

        String clientXml = asXmlString(clientConfig);
        params.setClientXml(clientXml);

        return writeMapSupplier(params);
    }

    /**
     * Update map with key and value functions
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier writeMapSupplier(MapSinkConfiguration<T, K, V> sinkConfig) {
        // Create ProcessorSupplier
        WriteMapP.Supplier<? super T, ? extends K, ? extends V> supplier = WriteMapP.Supplier.createNew(sinkConfig);
        if (sinkConfig.isRemote()) {
            // ProcessorMetaSupplier uses default local parallelism and no permission
            return preferLocalParallelismOne(supplier);
        } else {
            // Create permission for ProcessorMetaSupplier
            Permission processorMetaSupplierPermission = mapPutPermission(sinkConfig.getMapName());

            // ProcessorMetaSupplier uses default local parallelism
            return preferLocalParallelismOne(processorMetaSupplierPermission, supplier);
        }
    }

    /**
     * Update map with a merge function
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier mergeMapSupplier(MapSinkConfiguration<T, K, V> sinkConfig) {
        // Get reference to functions because MapSinkMergeParams is not serializable
        FunctionEx<? super T, ? extends K> toKeyFn = sinkConfig.getToKeyFn();
        FunctionEx<? super T, ? extends V> toValueFn = sinkConfig.getToValueFn();
        BinaryOperatorEx<V> mergeFn = sinkConfig.getMergeFn();

        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(toValueFn, "toValueFn");
        checkSerializable(mergeFn, "mergeFn");

        boolean isRemote = sinkConfig.isRemote();
        FunctionEx<HecatonInstance, Processor> processorFunction = SecuredFunctions.updateMapProcessorFn(
                sinkConfig.getMapName(),
                isRemote,
                toKeyFn,
                (V oldValue, T item) -> {
                    V newValue = toValueFn.apply(item);
                    if (oldValue == null) {
                        return newValue;
                    }
                    return mergeFn.apply(oldValue, newValue);
                });

        ProcessorFunctionConnectorSupplier processorSupplier = new ProcessorFunctionConnectorSupplier(
                sinkConfig.getDataConnectionName(),
                sinkConfig.getClientXml(),
                processorFunction
        );

        if (isRemote) {
            // ProcessorMetaSupplier uses default local parallelism and no permission
            return ProcessorMetaSupplier.of(processorSupplier);
        } else {
            // Create permission for ProcessorMetaSupplier
            Permission processorMetaSupplierPermission = mapUpdatePermission(sinkConfig.getMapName());

            // ProcessorMetaSupplier uses default local parallelism
            return ProcessorMetaSupplier.of(processorMetaSupplierPermission, processorSupplier);
        }
    }

    /**
     * Update map with an update function
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier updateMapSupplier(
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull BiFunctionEx<? super V, ? super T, ? extends V> updateFn
    ) {
        checkSerializable(toKeyFn, "toKeyFn");
        checkSerializable(updateFn, "updateFn");

        MapSinkConfiguration<T, K, V> sinkConfig = new MapSinkConfiguration<>(name);
        sinkConfig.setClientXml(ImdgUtil.asXmlString(clientConfig));
        sinkConfig.setToKeyFn(toKeyFn);
        sinkConfig.setUpdateFn(updateFn);

        return updateMapSupplier(sinkConfig);
    }

    /**
     * Update map with an update function
     */
    @Nonnull
    public static <T, K, V> ProcessorMetaSupplier updateMapSupplier(MapSinkConfiguration<T, K, V> sinkConfig) {
        checkSerializable(sinkConfig.getToKeyFn(), "toKeyFn");
        checkSerializable(sinkConfig.getUpdateFn(), "updateFn");

        boolean isRemote = sinkConfig.isRemote();
        FunctionEx<HecatonInstance, Processor> processorFunction = SecuredFunctions.updateMapProcessorFn(
                sinkConfig.getMapName(),
                isRemote,
                sinkConfig.getToKeyFn(),
                sinkConfig.getUpdateFn());

        ProcessorFunctionConnectorSupplier processorSupplier = new ProcessorFunctionConnectorSupplier(
                sinkConfig.getDataConnectionName(),
                sinkConfig.getClientXml(),
                processorFunction
        );

        if (isRemote) {
            // ProcessorMetaSupplier uses default local parallelism and no permission
            return ProcessorMetaSupplier.of(processorSupplier);
        } else {
            // Create permission for ProcessorMetaSupplier
            Permission processorMetaSupplierPermission = mapUpdatePermission(sinkConfig.getMapName());

            // ProcessorMetaSupplier uses default local parallelism
            return ProcessorMetaSupplier.of(processorMetaSupplierPermission, processorSupplier);
        }
    }

    /**
     * Update map with an EntryProcessor
     */
    @Nonnull
    public static <T, K, V, R> ProcessorMetaSupplier updateMapSupplier(
            @Nonnull String name,
            @Nullable ClientConfig clientConfig,
            @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
            @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn
    ) {
        MapSinkEntryProcessorConfiguration<T, K, V, R> sinkConfig = new MapSinkEntryProcessorConfiguration<>(name);
        sinkConfig.setClientXml(ImdgUtil.asXmlString(clientConfig));
        sinkConfig.setMaxParallelAsyncOps(MAX_PARALLEL_ASYNC_OPS_DEFAULT);
        sinkConfig.setToKeyFn(toKeyFn);
        sinkConfig.setToEntryProcessorFn(toEntryProcessorFn);

        return updateMapSupplier(sinkConfig);

    }

    /**
     * Update map with an EntryProcessor
     */
    @Nonnull
    public static <T, K, V, R> ProcessorMetaSupplier updateMapSupplier(
            MapSinkEntryProcessorConfiguration<T, K, V, R> sinkConfig
    ) {
        checkSerializable(sinkConfig.getToKeyFn(), "toKeyFn");
        checkSerializable(sinkConfig.getToEntryProcessorFn(), "toEntryProcessorFn");

        boolean isRemote = sinkConfig.isRemote();
        FunctionEx<HecatonInstance, Processor> processorFunction = SecuredFunctions.updateWithEntryProcessorFn(
                sinkConfig.getMaxParallelAsyncOps(),
                sinkConfig.getMapName(),
                isRemote,
                sinkConfig.getToKeyFn(),
                sinkConfig.getToEntryProcessorFn());

        ProcessorFunctionConnectorSupplier processorSupplier = new ProcessorFunctionConnectorSupplier(
                sinkConfig.getDataConnectionName(),
                sinkConfig.getClientXml(),
                processorFunction
        );
        if (isRemote) {
            // ProcessorMetaSupplier uses default local parallelism and no permission
            return ProcessorMetaSupplier.of(processorSupplier);
        } else {
            // Create permission for ProcessorMetaSupplier
            Permission processorMetaSupplierPermission = mapUpdatePermission(sinkConfig.getMapName());
            // ProcessorMetaSupplier uses default local parallelism
            return ProcessorMetaSupplier.of(processorMetaSupplierPermission, processorSupplier);
        }
    }

    @Nonnull
    public static ProcessorMetaSupplier writeCacheSupplier(@Nonnull String name, @Nullable ClientConfig clientConfig) {
        String clientXml = asXmlString(clientConfig);
        return preferLocalParallelismOne(cachePutPermission(clientXml, name),
                new WriteCachePSupplier<>(clientXml, name));
    }

    @Nonnull
    public static ProcessorMetaSupplier writeListSupplier(@Nonnull String name, @Nullable ClientConfig clientConfig) {
        String clientXml = asXmlString(clientConfig);
        return preferLocalParallelismOne(listAddPermission(clientXml, name),
                new WriteListPSupplier<>(clientXml, name));
    }

    @SuppressWarnings("AnonInnerLength")
    public static ProcessorMetaSupplier writeObservableSupplier(@Nonnull String name) {
        return new ProcessorMetaSupplier() {
            @Serial
            private static final long serialVersionUID = 1L;

            @Nonnull
            @Override
            public Map<String, String> getTags() {
                return singletonMap(ObservableImpl.OWNED_OBSERVABLE, name);
            }

            @Override
            public int preferredLocalParallelism() {
                return 1;
            }

            @Nonnull @Override
            public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
                return address -> new WriteObservableP.Supplier(name);
            }

            @Override
            public Permission getRequiredPermission() {
                return new RingBufferPermission(name, ACTION_CREATE, ACTION_PUT);
            }

            @Override
            public boolean isReusable() {
                return true;
            }

            @Override
            public boolean initIsCooperative() {
                return true;
            }

            @Override
            public boolean closeIsCooperative() {
                return true;
            }
        };
    }

    static RuntimeException handleInstanceNotActive(HecatonInstanceNotActiveException e, boolean isLocal) {
        // if we are writing to a local instance, restarting the job should resolve the error
        return isLocal ? new RestartableException(e) : e;
    }

    private static class WriteCachePSupplier<K, V> extends AbstractHecatonConnectorSupplier {

        @Serial
        private static final long serialVersionUID = 1L;

        private final String name;

        WriteCachePSupplier(@Nullable String clientXml, @Nonnull String name) {
            super(null, clientXml);
            this.name = name;
        }

        @Override
        protected Processor createProcessor(HecatonInstance instance, SerializationService serializationService) {
            ICache<Data, Data> cache = instance.getCacheManager().getCache(name);

            FunctionEx<Context, ArrayMap<Data, Data>> bufferCreator = context -> new ArrayMap<>();
            BiConsumerEx<ArrayMap<Data, Data>, Entry<K, V>> entryReceiver = (buffer, entry) -> {
                Data key = serializationService.toData(entry.getKey());
                Data value = serializationService.toData(entry.getValue());
                buffer.add(new SimpleEntry<>(key, value));
            };
            ConsumerEx<ArrayMap<Data, Data>> bufferFlusher = buffer -> {
                try {
                    cache.putAll(buffer);
                } catch (HecatonInstanceNotActiveException e) {
                    throw handleInstanceNotActive(e, isLocal());
                }
                buffer.clear();
            };

            return new WriteBufferedP<>(bufferCreator, entryReceiver, bufferFlusher, ConsumerEx.noop());
        }

        @Override
        public List<Permission> permissions() {
            return singletonList(cachePutPermission(clientXml, name));
        }
    }

    private static class WriteListPSupplier<T> extends AbstractHecatonConnectorSupplier {

        @Serial
        private static final long serialVersionUID = 1L;

        private final String name;

        WriteListPSupplier(@Nullable String clientXml, @Nonnull String name) {
            super(null, clientXml);
            this.name = name;
        }

        @Override
        protected Processor createProcessor(HecatonInstance instance, SerializationService serializationService) {
            IList<Object> list = instance.getList(name);

            FunctionEx<Context, List<Data>> bufferCreator = context -> new ArrayList<>();
            BiConsumerEx<List<Data>, T> itemReceiver = (buffer, item) -> buffer.add(serializationService.toData(item));
            ConsumerEx<List<Data>> bufferFlusher = buffer -> {
                try {
                    list.addAll(buffer);
                } catch (HecatonInstanceNotActiveException e) {
                    throw handleInstanceNotActive(e, isLocal());
                }
                buffer.clear();
            };

            return new WriteBufferedP<>(bufferCreator, itemReceiver, bufferFlusher, ConsumerEx.noop());
        }

        @Override
        public List<Permission> permissions() {
            return singletonList(listAddPermission(clientXml, name));
        }
    }

    public static final class ArrayMap<K, V> extends AbstractMap<K, V> {

        private final List<Entry<K, V>> entries;
        private final ArraySet set = new ArraySet();

        ArrayMap() {
            entries = new ArrayList<>();
        }

        public ArrayMap(int size) {
            entries = new ArrayList<>(size);
        }

        @Override @Nonnull
        public Set<Entry<K, V>> entrySet() {
            return set;
        }

        public void add(Map.Entry<K, V> entry) {
            entries.add(entry);
        }

        @Override
        public V get(Object key) {
            throw new UnsupportedOperationException();
        }

        private class ArraySet extends AbstractSet<Entry<K, V>> {

            @Override @Nonnull
            public Iterator<Entry<K, V>> iterator() {
                return entries.iterator();
            }

            @Override
            public int size() {
                return entries.size();
            }

            @Override
            public void clear() {
                entries.clear();
            }
        }

        @Override
        public String toString() {
            return entries.toString();
        }
    }
}
