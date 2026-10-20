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

package uk.org.hecaton.jet.sql.impl.connector.keyvalue;

import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.core.ResettableSingletonTraverser;
import uk.org.hecaton.jet.impl.processor.TransformP;
import uk.org.hecaton.jet.sql.impl.inject.UpsertTargetDescriptor;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import uk.org.hecaton.sql.impl.row.JetSqlRow;
import uk.org.hecaton.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class KvProcessors {

    private KvProcessors() {
    }

    /**
     * Returns a supplier of processors that convert a row represented as
     * {@link JetSqlRow} to an entry represented as {@code Entry<Object,
     * Object>}.
     */
    public static ProcessorSupplier entryProjector(
            QueryPath[] paths,
            QueryDataType[] types,
            UpsertTargetDescriptor keyDescriptor,
            UpsertTargetDescriptor valueDescriptor,
            boolean failOnNulls
    ) {
        return new EntryProjectorProcessorSupplier(KvProjector.supplier(
                paths,
                types,
                keyDescriptor,
                valueDescriptor,
                failOnNulls
        ));
    }

//    @SuppressFBWarnings(
//            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
//            justification = "the class is never java-serialized"
//    )
    private static final class EntryProjectorProcessorSupplier implements ProcessorSupplier, DataSerializable {

        private KvProjector.Supplier projectorSupplier;

        private transient ExpressionEvalContext evalContext;

        @SuppressWarnings("unused")
        private EntryProjectorProcessorSupplier() {
        }

        EntryProjectorProcessorSupplier(KvProjector.Supplier projectorSupplier) {
            this.projectorSupplier = projectorSupplier;
        }

        @Override
        public void init(@Nonnull Context context) {
            evalContext = ExpressionEvalContext.from(context);
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            List<Processor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                ResettableSingletonTraverser<Object> traverser = new ResettableSingletonTraverser<>();
                KvProjector projector = projectorSupplier.get(evalContext);
                Processor processor = new TransformP<JetSqlRow, Object>(row -> {
                    traverser.accept(projector.project(row));
                    return traverser;
                });
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
    }
}
