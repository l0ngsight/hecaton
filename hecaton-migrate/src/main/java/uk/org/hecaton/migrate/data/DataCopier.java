/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.migrate.data;

import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MapFetchEntriesCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapEntrySetCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapEntrySetCodec;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.iteration.IterationPointer;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.SerializationConstants;
import uk.org.hecaton.internal.util.ExceptionUtil;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.multimap.MultiMap;
import uk.org.hecaton.replicatedmap.ReplicatedMap;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static uk.org.hecaton.internal.iteration.IterationPointer.decodePointers;
import static uk.org.hecaton.internal.iteration.IterationPointer.encodePointers;

/**
 * Copies IMap, MultiMap and ReplicatedMap contents from one cluster to another through two
 * clients (plan §1.5): from a Hazelcast 5.x cluster, which the Hecaton client can connect to
 * because the client protocol is unchanged (spec §5.4), to a Hecaton cluster.
 * <p>
 * Entries are copied in their serialized form, so the copier needs none of the application's
 * classes. Compact values are the exception: their schemas live in the source cluster, so they
 * are read as {@code GenericRecord}s and written again, which registers the schemas with the
 * target cluster. Entries are copied as they are while the source keeps changing; stop writes to
 * the source first for an exact copy.
 */
public final class DataCopier {

    /** Service names of the copied structures. */
    static final String MAP_SERVICE = "hz:impl:mapService";
    static final String MULTI_MAP_SERVICE = "hz:impl:multiMapService";
    static final String REPLICATED_MAP_SERVICE = "hz:impl:replicatedMapService";

    private final HecatonInstance source;
    private final HecatonInstance target;
    private final int batchSize;
    private final PrintStream out;

    /**
     * A copied structure.
     *
     * @param service its service name
     * @param name    its name
     * @param entries the number of entries copied
     */
    public record Copied(String service, String name, long entries) {
    }

    public DataCopier(HecatonInstance source, HecatonInstance target, int batchSize, PrintStream out) {
        this.source = source;
        this.target = target;
        this.batchSize = batchSize;
        this.out = out;
    }

    /**
     * Copies the named structures, or every IMap, MultiMap and ReplicatedMap of the source
     * (except internal ones, whose names start with {@code __}) when {@code names} is empty.
     */
    public List<Copied> copy(Set<String> names) {
        List<Copied> copied = new ArrayList<>();
        for (DistributedObject object : source.getDistributedObjects()) {
            String name = object.getName();
            String service = object.getServiceName();
            boolean selected = names.isEmpty() ? !name.startsWith("__") : names.contains(name);
            if (!selected) {
                continue;
            }
            long entries = switch (service) {
                case MAP_SERVICE -> copyMap(name);
                case MULTI_MAP_SERVICE -> copyMultiMap(name);
                case REPLICATED_MAP_SERVICE -> copyReplicatedMap(name);
                default -> -1;
            };
            if (entries >= 0) {
                out.println("copied " + entries + " entries of " + kind(service) + " " + name);
                copied.add(new Copied(service, name, entries));
            }
        }
        return copied;
    }

    long copyMap(String name) {
        HecatonClientInstanceImpl client = clientImpl(source);
        IMap<Object, Object> targetMap = target.getMap(name);
        int partitionCount = client.getClientPartitionService().getPartitionCount();
        long count = 0;
        for (int partition = 0; partition < partitionCount; partition++) {
            IterationPointer[] pointers = {new IterationPointer(Integer.MAX_VALUE, -1)};
            while (true) {
                ClientMessage request = MapFetchEntriesCodec.encodeRequest(name, encodePointers(pointers), batchSize);
                MapFetchEntriesCodec.ResponseParameters response =
                        MapFetchEntriesCodec.decodeResponse(invoke(client, request, name, partition));
                if (response.entries.isEmpty()) {
                    break;
                }
                Map<Object, Object> batch = new LinkedHashMap<>();
                for (Map.Entry<Data, Data> entry : response.entries) {
                    batch.put(portable(entry.getKey()), portable(entry.getValue()));
                }
                targetMap.putAll(batch);
                count += batch.size();
                pointers = decodePointers(response.iterationPointers);
                if (pointers[pointers.length - 1].getIndex() < 0) {
                    break;
                }
            }
        }
        return count;
    }

    long copyMultiMap(String name) {
        HecatonClientInstanceImpl client = clientImpl(source);
        MultiMap<Object, Object> targetMultiMap = target.getMultiMap(name);
        List<Map.Entry<Data, Data>> entries = MultiMapEntrySetCodec.decodeResponse(
                invoke(client, MultiMapEntrySetCodec.encodeRequest(name), name, -1));
        for (Map.Entry<Data, Data> entry : entries) {
            targetMultiMap.put(portable(entry.getKey()), portable(entry.getValue()));
        }
        return entries.size();
    }

    long copyReplicatedMap(String name) {
        HecatonClientInstanceImpl client = clientImpl(source);
        ReplicatedMap<Object, Object> targetReplicatedMap = target.getReplicatedMap(name);
        // every member holds the whole replicated map; the request runs on the owner of any partition
        List<Map.Entry<Data, Data>> entries = ReplicatedMapEntrySetCodec.decodeResponse(
                invoke(client, ReplicatedMapEntrySetCodec.encodeRequest(name), name, 0));
        Map<Object, Object> all = new LinkedHashMap<>();
        for (Map.Entry<Data, Data> entry : entries) {
            all.put(portable(entry.getKey()), portable(entry.getValue()));
        }
        targetReplicatedMap.putAll(all);
        return all.size();
    }

    /**
     * What to give the target client for {@code data}: the serialized form itself, which the
     * client passes on unchanged, or for Compact data the deserialized record, so that the
     * target client registers its schema.
     */
    private Object portable(Data data) {
        if (data == null) {
            return null;
        }
        int type = data.getType();
        if (type == SerializationConstants.TYPE_COMPACT || type == SerializationConstants.TYPE_COMPACT_WITH_SCHEMA) {
            InternalSerializationService serialization = clientImpl(source).getSerializationService();
            return serialization.toObject(data);
        }
        return data;
    }

    private static ClientMessage invoke(HecatonClientInstanceImpl client, ClientMessage request, String name,
                                        int partition) {
        ClientInvocation invocation = partition >= 0
                ? new ClientInvocation(client, request, name, partition)
                : new ClientInvocation(client, request, name);
        try {
            return invocation.invoke().get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private static HecatonClientInstanceImpl clientImpl(HecatonInstance instance) {
        if (instance instanceof HecatonClientProxy proxy) {
            return proxy.client;
        }
        throw new IllegalArgumentException("The source must be a client, not " + instance);
    }

    private static String kind(String service) {
        return switch (service) {
            case MAP_SERVICE -> "map";
            case MULTI_MAP_SERVICE -> "multimap";
            case REPLICATED_MAP_SERVICE -> "replicated map";
            default -> service;
        };
    }
}
