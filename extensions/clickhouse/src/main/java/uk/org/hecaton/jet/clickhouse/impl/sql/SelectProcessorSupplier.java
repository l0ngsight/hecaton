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

package uk.org.hecaton.jet.clickhouse.impl.sql;

import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.dataconnection.DataConnectionService;
import uk.org.hecaton.jet.Traverser;
import uk.org.hecaton.jet.clickhouse.impl.ClientSource;
import uk.org.hecaton.jet.clickhouse.impl.ColumnType;
import uk.org.hecaton.jet.core.AbstractProcessor;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.sql.impl.ExpressionUtil;
import uk.org.hecaton.sql.impl.row.JetSqlRow;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static uk.org.hecaton.jet.pipeline.DataConnectionRef.dataConnectionRef;

/**
 * Runs the SELECT of a scan on each processor, and evaluates the scan's predicate and
 * projection on the rows it returns.
 */
final class SelectProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private String dataConnection;
    private String sql;
    private List<PushedParameter> parameters;
    private boolean split;
    private int[] selectedFields;
    private List<QueryDataType> types;
    private Expression<Boolean> predicate;
    private List<Expression<?>> projection;

    private transient ExpressionEvalContext evalContext;
    private transient DataConnectionService dataConnectionService;
    private transient int firstGlobalIndex;
    private transient int totalParallelism;

    @SuppressWarnings("unused")
    private SelectProcessorSupplier() {
    }

    /**
     * @param sql            the query; with {@code split}, it ends with the condition that
     *                       selects the rows of a processor, with the parameters {@code total}
     *                       and {@code index}
     * @param selectedFields the fields the query returns, in its order
     * @param types          the type of every field of the table
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    SelectProcessorSupplier(String dataConnection, String sql, List<PushedParameter> parameters, boolean split,
                            int[] selectedFields, List<QueryDataType> types, Expression<Boolean> predicate,
                            List<Expression<?>> projection) {
        this.dataConnection = dataConnection;
        this.sql = sql;
        this.parameters = parameters;
        this.split = split;
        this.selectedFields = selectedFields;
        this.types = types;
        this.predicate = predicate;
        this.projection = projection;
    }

    @Override
    public void init(@Nonnull Context context) {
        evalContext = ExpressionEvalContext.from(context);
        dataConnectionService = context.dataConnectionService();
        firstGlobalIndex = context.memberIndex() * context.localParallelism();
        totalParallelism = context.totalParallelism();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Map<String, Object> queryParameters = new HashMap<>();
            for (int p = 0; p < parameters.size(); p++) {
                queryParameters.put("p" + p, parameters.get(p).value(evalContext.getArguments()));
            }
            if (split) {
                queryParameters.put("total", totalParallelism);
                queryParameters.put("index", firstGlobalIndex + i);
            }
            processors.add(new SelectP(queryParameters));
        }
        return processors;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(dataConnection);
        out.writeString(sql);
        out.writeObject(parameters);
        out.writeBoolean(split);
        out.writeIntArray(selectedFields);
        out.writeObject(types);
        out.writeObject(predicate);
        out.writeObject(projection);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataConnection = in.readString();
        sql = in.readString();
        parameters = in.readObject();
        split = in.readBoolean();
        selectedFields = in.readIntArray();
        types = in.readObject();
        predicate = in.readObject();
        projection = in.readObject();
    }

    /**
     * Reads the query's rows as the outbox takes them.
     */
    private final class SelectP extends AbstractProcessor {

        private final Map<String, Object> queryParameters;
        private ClientSource.Lease lease;
        private QueryResponse response;
        private ClickHouseBinaryFormatReader reader;
        private ColumnType[] columnTypes;
        private Traverser<JetSqlRow> traverser;

        SelectP(Map<String, Object> queryParameters) {
            this.queryParameters = queryParameters;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        protected void init(@Nonnull Context context) {
            lease = ClientSource.of(dataConnectionRef(dataConnection)).open(dataConnectionService);
            Client client = lease.client();
            try {
                response = client.query(sql, queryParameters).get();
                reader = client.newBinaryFormatReader(response);
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new HecatonException("The ClickHouse query failed: " + e.getMessage() + "; query: " + sql, e);
            }
            columnTypes = new ColumnType[selectedFields.length];
            for (int i = 0; i < selectedFields.length; i++) {
                columnTypes[i] = ColumnType.of(reader.getSchema().getColumnByIndex(i + 1));
            }
            traverser = this::nextRow;
        }

        private JetSqlRow nextRow() {
            while (reader.hasNext()) {
                reader.next();
                Object[] values = new Object[types.size()];
                for (int i = 0; i < selectedFields.length; i++) {
                    int field = selectedFields[i];
                    values[field] = types.get(field).convert(columnTypes[i].fromClickHouse(reader.readValue(i + 1)));
                }
                JetSqlRow row = ExpressionUtil.evaluate(predicate, projection,
                        new JetSqlRow(evalContext.getSerializationService(), values), evalContext);
                if (row != null) {
                    return row;
                }
            }
            return null;
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(traverser);
        }

        @Override
        public void close() throws Exception {
            try {
                if (reader != null) {
                    reader.close();
                }
                if (response != null) {
                    response.close();
                }
            } finally {
                if (lease != null) {
                    lease.close();
                }
            }
        }
    }
}
