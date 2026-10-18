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

import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.proxy.ClientListProxy;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.impl.list.ListProxyImpl;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.jet.Traverser;
import uk.org.hecaton.jet.core.AbstractProcessor;
import uk.org.hecaton.jet.impl.execution.init.Contexts.ProcCtx;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.BiFunction;

import static uk.org.hecaton.client.HecatonClient.newHecatonClient;
import static uk.org.hecaton.jet.Traversers.traverseIterable;
import static uk.org.hecaton.jet.Traversers.traverseStream;
import static uk.org.hecaton.jet.impl.util.ImdgUtil.asClientConfig;
import static java.lang.Math.min;
import static java.util.stream.IntStream.rangeClosed;

public final class ReadIListP extends AbstractProcessor {

    static final int FETCH_SIZE = 16384;

    private final String name;
    private final String clientXml;

    private Traverser<Object> traverser;
    private HecatonInstance client;

    public ReadIListP(String name, String clientXml) {
        this.name = name;
        this.clientXml = clientXml;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected void init(@Nonnull Context context) {
        HecatonInstance instance;
        SerializationService serializationService;
        if (isRemote()) {
            instance = client = newHecatonClient(asClientConfig(clientXml));
            serializationService = ((HecatonClientProxy) instance).getSerializationService();
        } else {
            instance = context.hecatonInstance();
            serializationService = ((ProcCtx) context).serializationService();
        }
        traverser = createTraverser(instance, name).map(serializationService::toObject);
    }

    private Traverser<Data> createTraverser(HecatonInstance instance, String name) {
        IList<Data> list = instance.getList(name);
        int size = list.size();

        if (list instanceof ClientListProxy proxy) {
            return createTraverser(size, proxy::dataSubList);
        } else if (list instanceof ListProxyImpl proxy) {
            return createTraverser(size, proxy::dataSubList);
        } else {
            throw new RuntimeException("Unexpected list class: " + list.getClass().getName());
        }
    }

    private Traverser<Data> createTraverser(int size,
                                            BiFunction<Integer, Integer, List<Data>> subListSupplier) {
        return size <= FETCH_SIZE ?
                traverseIterable(subListSupplier.apply(0, size)) :
                traverseStream(rangeClosed(0, size / FETCH_SIZE).mapToObj(chunkIndex -> chunkIndex * FETCH_SIZE))
                        .flatMap(start -> traverseIterable(subListSupplier.apply(start, min(start + FETCH_SIZE, size))));
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    @Override
    public void close() {
        if (client != null) {
            client.shutdown();
        }
    }

    private boolean isRemote() {
        return clientXml != null;
    }
}
