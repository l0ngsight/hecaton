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

import uk.org.hecaton.function.BiFunctionEx;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.impl.connector.ReadJdbcP;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.security.impl.function.SecuredFunction;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("checkstyle:ExecutableStatementCount")
public class SelectProcessorSupplier
        extends AbstractJdbcSqlConnectorProcessorSupplier
        implements ProcessorSupplier, DataSerializable, SecuredFunction {

    private String query;
    private int[] parameterPositions;
    private List<FunctionEx<Object, ?>> converters = new ArrayList<>();

    private transient ExpressionEvalContext evalContext;
    private transient volatile BiFunctionEx<ResultSet, Integer, ?>[] valueGetters;

    @SuppressWarnings("unused")
    public SelectProcessorSupplier() {
    }

    public SelectProcessorSupplier(@Nonnull String dataConnectionName,
                                   @Nonnull String query,
                                   @Nonnull int[] parameterPositions,
                                   @Nonnull List<FunctionEx<Object, ?>> converters) {
        super(dataConnectionName);
        this.query = requireNonNull(query, "query must not be null");
        this.parameterPositions = requireNonNull(parameterPositions, "parameterPositions must not be null");
        this.converters = converters;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        super.init(context);
        this.evalContext = ExpressionEvalContext.from(context);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        assert count == 1;

        Processor processor = new ReadJdbcP<>(
                () -> dataConnection.getConnection(),
                (connection, parallelism, index) -> {
                    TypeResolver typeResolver = JdbcSqlConnector.typeResolver(connection);

                    PreparedStatement statement = connection.prepareStatement(query);
                    List<Object> arguments = evalContext.getArguments();
                    for (int j = 0; j < parameterPositions.length; j++) {
                        typeResolver.setObject(statement, arguments.get(parameterPositions[j]), j);
                    }
                    try {
                        ResultSet rs = statement.executeQuery();
                        valueGetters = JdbcSqlConnector.prepareValueGettersFromMetadata(typeResolver, rs, converters::get);
                        return rs;
                    } catch (SQLException e) {
                        statement.close();
                        throw e;
                    }
                },
                rs -> {
                    int columnCount = rs.getMetaData().getColumnCount();
                    Object[] row = new Object[columnCount];
                    for (int j = 0; j < columnCount; j++) {
                        Object value = valueGetters[j].apply(rs, j + 1);
                        row[j] = value;
                    }

                    return new JetSqlRow(evalContext.getSerializationService(), row);
                }
        );

        return singleton(processor);
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(dataConnectionName);
        out.writeString(query);
        out.writeIntArray(parameterPositions);
        out.writeObject(converters);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataConnectionName = in.readString();
        query = in.readString();
        parameterPositions = in.readIntArray();
        converters = in.readObject();
    }

}
