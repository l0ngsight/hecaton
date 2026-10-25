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

import uk.org.hecaton.dataconnection.DataConnectionService;
import uk.org.hecaton.jet.clickhouse.impl.ClientSource;
import uk.org.hecaton.jet.clickhouse.impl.ColumnType;
import uk.org.hecaton.jet.clickhouse.impl.RowInserter;
import uk.org.hecaton.jet.clickhouse.impl.TableMetadata;
import uk.org.hecaton.jet.clickhouse.impl.TableName;
import uk.org.hecaton.jet.core.AbstractProcessor;
import uk.org.hecaton.jet.core.Inbox;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.sql.impl.row.JetSqlRow;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static uk.org.hecaton.jet.pipeline.DataConnectionRef.dataConnectionRef;

/**
 * Inserts the rows of {@code INSERT INTO} and {@code SINK INTO} in batches: each batch is what
 * the inbox holds, up to the batch size.
 */
final class InsertProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private static final int INSERT_ATTEMPTS = 3;

    private String dataConnection;
    private TableName tableName;
    private List<String> columns;
    private int batchSize;
    private Map<String, String> settings;

    private transient DataConnectionService dataConnectionService;

    @SuppressWarnings("unused")
    private InsertProcessorSupplier() {
    }

    InsertProcessorSupplier(String dataConnection, TableName tableName, List<String> columns, int batchSize,
                            Map<String, String> settings) {
        this.dataConnection = dataConnection;
        this.tableName = tableName;
        this.columns = columns;
        this.batchSize = batchSize;
        this.settings = settings;
    }

    @Override
    public void init(@Nonnull Context context) {
        dataConnectionService = context.dataConnectionService();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            processors.add(new InsertP());
        }
        return processors;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(dataConnection);
        out.writeObject(tableName);
        out.writeObject(columns);
        out.writeInt(batchSize);
        out.writeObject(settings);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataConnection = in.readString();
        tableName = in.readObject();
        columns = in.readObject();
        batchSize = in.readInt();
        settings = in.readObject();
    }

    private final class InsertP extends AbstractProcessor {

        private ClientSource.Lease lease;
        private RowInserter inserter;

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        protected void init(@Nonnull Context context) {
            lease = ClientSource.of(dataConnectionRef(dataConnection)).open(dataConnectionService);
            TableMetadata metadata = TableMetadata.read(lease.client(), tableName);
            List<ColumnType> types = new ArrayList<>();
            for (String column : columns) {
                types.add(metadata.column(column));
            }
            inserter = new RowInserter(tableName, types, settings, INSERT_ATTEMPTS);
        }

        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            while (!inbox.isEmpty()) {
                List<Object[]> rows = new ArrayList<>(Math.min(inbox.size(), batchSize));
                for (JetSqlRow row; rows.size() < batchSize && (row = (JetSqlRow) inbox.poll()) != null; ) {
                    Object[] values = new Object[row.getFieldCount()];
                    for (int i = 0; i < values.length; i++) {
                        values[i] = row.get(i);
                    }
                    rows.add(inserter.convert(values));
                }
                inserter.insert(lease.client(), rows);
            }
        }

        @Override
        public void close() {
            if (lease != null) {
                lease.close();
            }
        }
    }
}
