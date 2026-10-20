/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.jet.sql.impl.connector.map;

import uk.org.hecaton.jet.Traverser;
import uk.org.hecaton.jet.Traversers;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.impl.processor.AsyncTransformUsingServiceBatchedP;
import uk.org.hecaton.jet.impl.util.Util;
import uk.org.hecaton.jet.pipeline.ServiceFactories;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.security.impl.function.SecuredFunctions;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.expression.UntrustedExpressionEvalContext;
import uk.org.hecaton.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.security.permission.ActionConstants.ACTION_CREATE;
import static uk.org.hecaton.security.permission.ActionConstants.ACTION_PUT;
import static java.util.Collections.singletonList;

final class UpdateProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private static final int MAX_CONCURRENT_OPS = 8;
    private static final int MAX_BATCH_SIZE = 1024;

    private String mapName;
    private UpdatingEntryProcessor.Supplier updaterSupplier;

    private transient ExpressionEvalContext evalContext;

    @SuppressWarnings("unused")
    private UpdateProcessorSupplier() {
    }

    UpdateProcessorSupplier(
            String mapName,
            UpdatingEntryProcessor.Supplier updaterSupplier
    ) {
        this.mapName = mapName;
        this.updaterSupplier = updaterSupplier;
    }

    @Override
    public void init(@Nonnull Context context) {
        evalContext = ExpressionEvalContext.from(context)
                // IMap updates will be executed in partition thread
                // and might be executed on local or remote instance.
                // Remote execution will not be able to use Jet job classloader,
                // so for consistency also local execution should not use it.
                .withSerializationService(Util.getSerializationService(context.hecatonInstance()));

    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String mapName = this.mapName;
            Processor processor = new AsyncTransformUsingServiceBatchedP<>(
                    ServiceFactories.nonSharedService(SecuredFunctions.iMapFn(mapName)),
                    null,
                    MAX_CONCURRENT_OPS,
                    MAX_BATCH_SIZE,
                    (IMap<Object, Object> map, List<JetSqlRow> rows) -> update(rows, map)
            );
            processors.add(processor);
        }
        return processors;
    }

    private CompletableFuture<Traverser<Integer>> update(List<JetSqlRow> rows, IMap<Object, Object> map) {
        Set<Object> keys = new HashSet<>();
        for (JetSqlRow row : rows) {
            assert row.getFieldCount() == 1;
            keys.add(row.get(0));
        }
        return map.submitToKeys(keys, updaterSupplier.get(UntrustedExpressionEvalContext.from(evalContext)))
                .toCompletableFuture()
                .thenApply(m -> Traversers.empty());
    }

    @Override
    public List<Permission> permissions() {
        return singletonList(new MapPermission(mapName, ACTION_CREATE, ACTION_PUT));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
        out.writeObject(updaterSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readString();
        updaterSupplier = in.readObject();
    }
}
