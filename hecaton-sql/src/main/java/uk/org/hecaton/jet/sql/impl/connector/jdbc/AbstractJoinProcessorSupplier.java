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

package uk.org.hecaton.jet.sql.impl.connector.jdbc;

import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.jet.impl.util.AutoCloseableTraverser;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.impl.processor.TransformBatchedAutoCloseableP;
import uk.org.hecaton.jet.sql.impl.JetJoinInfo;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

abstract class AbstractJoinProcessorSupplier
        extends AbstractJdbcSqlConnectorProcessorSupplier
        implements ProcessorSupplier, DataSerializable {

    protected String query;

    protected List<FunctionEx<Object, ?>> converters;

    //joinInfo is DataSerializable.
    protected JetJoinInfo joinInfo;

    //Expression is Serializable.
    protected List<Expression<?>> projections;

    // Transient members are set when ProcessorSupplier is initialized.
    // No need to serialize them
    protected transient ExpressionEvalContext expressionEvalContext;

    AbstractJoinProcessorSupplier() {
    }

    AbstractJoinProcessorSupplier(
            @Nonnull String dataConnectionName,
            @Nonnull String query,
            @Nonnull List<FunctionEx<Object, ?>> converters,
            @Nonnull JetJoinInfo joinInfo,
            List<Expression<?>> projections) {
        super(dataConnectionName);
        this.query = query;
        this.converters = converters;
        this.joinInfo = joinInfo;
        this.projections = projections;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        super.init(context);
        this.expressionEvalContext = ExpressionEvalContext.from(context);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        // Return a collection having count number of Processors
        return IntStream.range(0, count)
                // The processor is not cooperative due to blocking in joinRows
                .mapToObj(i -> new TransformBatchedAutoCloseableP<>(this::joinRows).setCooperative(false)).
                collect(toList());
    }

    /**
     * Return a Traverser that processes given rows
     */
    protected abstract AutoCloseableTraverser<JetSqlRow> joinRows(Iterable<JetSqlRow> leftRows);

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(dataConnectionName);
        out.writeString(query);
        out.writeObject(converters);
        out.writeObject(joinInfo);
        out.writeObject(projections);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataConnectionName = in.readString();
        query = in.readString();
        converters = in.readObject();
        joinInfo = in.readObject();
        projections = in.readObject();
    }
}
