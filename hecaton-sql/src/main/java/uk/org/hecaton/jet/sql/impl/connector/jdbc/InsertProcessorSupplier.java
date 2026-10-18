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

import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.impl.connector.WriteJdbcP;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.security.impl.function.SecuredFunction;
import uk.org.hecaton.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class InsertProcessorSupplier
        extends AbstractJdbcSqlConnectorProcessorSupplier
        implements DataSerializable, SecuredFunction {

    private String query;
    private int batchLimit;

    @SuppressWarnings("unused")
    public InsertProcessorSupplier() {
    }

    public InsertProcessorSupplier(String dataConnectionName, String query, int batchLimit) {
        super(dataConnectionName);
        this.query = query;
        this.batchLimit = batchLimit;
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Processor processor = new WriteJdbcP<>(
                    query,
                    dataSource,
                    (PreparedStatement ps, JetSqlRow row) -> {
                        TypeResolver typeResolver = JdbcSqlConnector.typeResolver(ps.getConnection());
                        for (int j = 0; j < row.getFieldCount(); j++) {
                            typeResolver.setObject(ps, row.get(j), j);
                        }
                    },
                    SQLExceptionUtils::isNonTransientException,
                    false,
                    batchLimit
            );
            processors.add(processor);
        }
        return processors;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(dataConnectionName);
        out.writeString(query);
        out.writeInt(batchLimit);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataConnectionName = in.readString();
        query = in.readString();
        batchLimit = in.readInt();
    }
}
