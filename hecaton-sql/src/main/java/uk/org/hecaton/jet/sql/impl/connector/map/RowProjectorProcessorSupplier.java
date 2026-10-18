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

import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.core.ResettableSingletonTraverser;
import uk.org.hecaton.jet.impl.processor.TransformP;
import uk.org.hecaton.jet.sql.impl.JetSqlSerializerHook;
import uk.org.hecaton.jet.sql.impl.connector.keyvalue.KvRowProjector;
import uk.org.hecaton.map.impl.LazyMapEntry;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import uk.org.hecaton.sql.impl.extract.QueryTargetDescriptor;
import uk.org.hecaton.sql.impl.row.JetSqlRow;
import uk.org.hecaton.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class RowProjectorProcessorSupplier implements ProcessorSupplier, IdentifiedDataSerializable {

    private KvRowProjector.Supplier projectorSupplier;

    private transient ExpressionEvalContext evalContext;
    private transient Extractors extractors;

    @SuppressWarnings("unused")
    public RowProjectorProcessorSupplier() {
    }

    public RowProjectorProcessorSupplier(KvRowProjector.Supplier projectorSupplier) {
        this.projectorSupplier = projectorSupplier;
    }

    @Override
    public void init(@Nonnull Context context) {
        evalContext = ExpressionEvalContext.from(context);
        extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ResettableSingletonTraverser<JetSqlRow> traverser = new ResettableSingletonTraverser<>();
            KvRowProjector projector = projectorSupplier.get(evalContext, extractors);
            TransformP<LazyMapEntry<Object, Object>, JetSqlRow> processor = new TransformP<>(entry -> {
                traverser.accept(projector.project(entry.getKeyData(), entry.getValueData()));
                return traverser;
            });
            processor.setCooperative(projector.isCooperative());
            processors.add(processor);
        }
        return processors;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(projectorSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        projectorSupplier = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.ROW_PROJECTOR_PROCESSOR_SUPPLIER;
    }

    public static ProcessorSupplier rowProjector(
            QueryPath[] paths,
            QueryDataType[] types,
            QueryTargetDescriptor keyDescriptor,
            QueryTargetDescriptor valueDescriptor,
            Expression<Boolean> predicate,
            List<Expression<?>> projection
    ) {
        return new RowProjectorProcessorSupplier(
                KvRowProjector.supplier(paths, types, keyDescriptor, valueDescriptor, predicate, projection));
    }
}
