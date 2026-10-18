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

package uk.org.hecaton.jet.sql.impl.connector.kafka;

import uk.org.hecaton.dataconnection.DataConnectionService;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.jet.core.EventTimePolicy;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.kafka.impl.StreamKafkaP;
import uk.org.hecaton.jet.pipeline.DataConnectionRef;
import uk.org.hecaton.jet.sql.impl.connector.keyvalue.KvRowProjector;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
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
import java.util.Properties;

import static java.util.Collections.singletonList;

//@SuppressFBWarnings(
//        value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
//        justification = "the class is never java-serialized"
//)
final class RowProjectorProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private Properties properties;
    private String dataConnectionName;
    private String topic;
    private FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider;
    private KvRowProjector.Supplier projectorSupplier;

    private transient ExpressionEvalContext evalContext;
    private transient EventTimePolicy<JetSqlRow> eventTimePolicy;
    private transient Extractors extractors;
    private transient DataConnectionService dataConnectionService;

    @SuppressWarnings("unused")
    private RowProjectorProcessorSupplier() {
    }

    RowProjectorProcessorSupplier(
            Properties properties,
            String dataConnectionName,
            String topic,
            FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider,
            QueryPath[] paths,
            QueryDataType[] types,
            QueryTargetDescriptor keyDescriptor,
            QueryTargetDescriptor valueDescriptor,
            Expression<Boolean> predicate,
            List<Expression<?>> projection
    ) {
        this.properties = properties;
        this.dataConnectionName = dataConnectionName;
        this.topic = topic;
        this.eventTimePolicyProvider = eventTimePolicyProvider;
        this.projectorSupplier = KvRowProjector.supplier(
                paths,
                types,
                keyDescriptor,
                valueDescriptor,
                predicate,
                projection
        );
    }

    @Override
    public void init(@Nonnull Context context) {
        evalContext = ExpressionEvalContext.from(context);
        eventTimePolicy = eventTimePolicyProvider == null
                ? EventTimePolicy.noEventTime()
                : eventTimePolicyProvider.apply(evalContext);
        extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
        dataConnectionService = context.dataConnectionService();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            KvRowProjector projector = projectorSupplier.get(evalContext, extractors);
            Processor processor = new StreamKafkaP<>(
                    (dataConnectionName == null) ? StreamKafkaP.kafkaConsumerFn(properties)
                            : StreamKafkaP.kafkaConsumerFn(new DataConnectionRef(dataConnectionName), properties),
                    singletonList(topic),
                    record -> projector.project(record.key(), record.value()),
                    eventTimePolicy
            );
            processors.add(processor);
        }
        return processors;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(properties);
        out.writeString(dataConnectionName);
        out.writeString(topic);
        out.writeObject(eventTimePolicyProvider);
        out.writeObject(projectorSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        properties = in.readObject();
        dataConnectionName = in.readString();
        topic = in.readString();
        eventTimePolicyProvider = in.readObject();
        projectorSupplier = in.readObject();
    }
}
