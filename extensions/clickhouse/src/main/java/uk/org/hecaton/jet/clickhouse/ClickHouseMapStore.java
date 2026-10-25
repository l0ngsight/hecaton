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

package uk.org.hecaton.jet.clickhouse;

import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.clickhouse.impl.ClientSource;
import uk.org.hecaton.jet.clickhouse.impl.ColumnType;
import uk.org.hecaton.jet.clickhouse.impl.RowInserter;
import uk.org.hecaton.jet.clickhouse.impl.TableMetadata;
import uk.org.hecaton.jet.clickhouse.impl.TableName;
import uk.org.hecaton.jet.impl.util.Util;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.MapLoaderLifecycleSupport;
import uk.org.hecaton.map.MapStore;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecord;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecordBuilder;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;

import java.io.Closeable;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Arrays.stream;
import static uk.org.hecaton.jet.pipeline.DataConnectionRef.dataConnectionRef;

/**
 * A {@link MapStore} that keeps the entries of a map in a ClickHouse table of the
 * {@code ReplacingMergeTree} family, usually {@code ReplicatedReplacingMergeTree}:
 * <pre>{@code
 * CREATE TABLE people ON CLUSTER my_cluster (
 *     id         Int64,
 *     name       String,
 *     age        Nullable(Int32),
 *     version    UInt64,
 *     is_deleted UInt8
 * ) ENGINE = ReplicatedReplacingMergeTree(version, is_deleted)
 * ORDER BY id
 * }</pre>
 * ClickHouse does not update rows in place: every store inserts a new row with a higher
 * {@code version}, and every delete inserts a row with {@code is_deleted = 1}. The table must be
 * ordered by the key column alone, so that ClickHouse keeps the latest row of each key. The map
 * store reads with {@code FINAL}, which returns only those rows, and skips deleted keys.
 * <p>
 * Values are compact {@link GenericRecord}s with one field per value column (the type name is
 * the map name, or the {@code type-name} property), or, with {@code single-column-as-value}, the
 * value of the only value column. Keys are the values of the key column. The Java types of the
 * columns are listed in the ClickHouse connector documentation.
 * <p>
 * Properties:
 * <ul>
 *     <li>{@code data-connection-ref}: the name of a {@link ClickHouseDataConnection}; or the
 *     properties of a ClickHouse data connection ({@code endpoints}, {@code username},
 *     {@code password}, {@code database}, {@code client.*}), for a client of this map store
 *     only;</li>
 *     <li>{@code external-name}: the table, {@code table} or {@code database.table} (default: the
 *     map name);</li>
 *     <li>{@code id-column} (default {@code id}), {@code version-column} (default
 *     {@code version}) and {@code is-deleted-column} (default {@code is_deleted});</li>
 *     <li>{@code columns}: the value columns, separated by commas (default: all other
 *     columns);</li>
 *     <li>{@code type-name}: the type name of the compact records (default: the map name);</li>
 *     <li>{@code single-column-as-value} (default {@code false});</li>
 *     <li>{@code load-all-keys} (default {@code true}): whether {@link #loadAllKeys()} returns
 *     the keys of the table, to load them when the map is first used;</li>
 *     <li>{@code insert-quorum}: the {@code insert_quorum} setting of inserts, for example
 *     {@code auto} for a majority of the replicas (default: not set, an insert succeeds once
 *     one replica has it);</li>
 *     <li>{@code select-sequential-consistency} (default {@code false}): reads only from
 *     replicas that have all inserts acknowledged with a quorum;</li>
 *     <li>{@code insert-attempts} (default 3): how many times an insert is tried.</li>
 * </ul>
 * The {@code version} of each row comes from the clock of the member that stores it, in
 * microseconds, and increases on each member. If an entry is stored on one member and, after a
 * migration, on another one whose clock is behind, the later store may lose; keep the members'
 * clocks synchronized.
 *
 * @param <K> the type of the keys
 * @param <V> the type of the values
 * @since 1.0
 */
public class ClickHouseMapStore<K, V> implements MapStore<K, V>, MapLoaderLifecycleSupport {

    /**
     * The name of the {@link ClickHouseDataConnection} to use.
     */
    public static final String DATA_CONNECTION_REF_PROPERTY = "data-connection-ref";
    /**
     * The table, {@code table} or {@code database.table}.
     */
    public static final String EXTERNAL_NAME_PROPERTY = "external-name";
    /**
     * The key column.
     */
    public static final String ID_COLUMN_PROPERTY = "id-column";
    /**
     * The version column of the {@code ReplacingMergeTree} engine.
     */
    public static final String VERSION_COLUMN_PROPERTY = "version-column";
    /**
     * The {@code is_deleted} column of the {@code ReplacingMergeTree} engine.
     */
    public static final String IS_DELETED_COLUMN_PROPERTY = "is-deleted-column";
    /**
     * The value columns, separated by commas.
     */
    public static final String COLUMNS_PROPERTY = "columns";
    /**
     * The type name of the compact records.
     */
    public static final String TYPE_NAME_PROPERTY = "type-name";
    /**
     * Whether the value is the only value column, instead of a record.
     */
    public static final String SINGLE_COLUMN_AS_VALUE_PROPERTY = "single-column-as-value";
    /**
     * Whether {@link #loadAllKeys()} returns the keys of the table.
     */
    public static final String LOAD_ALL_KEYS_PROPERTY = "load-all-keys";
    /**
     * The {@code insert_quorum} setting of inserts.
     */
    public static final String INSERT_QUORUM_PROPERTY = "insert-quorum";
    /**
     * Whether reads use the {@code select_sequential_consistency} setting.
     */
    public static final String SELECT_SEQUENTIAL_CONSISTENCY_PROPERTY = "select-sequential-consistency";
    /**
     * How many times an insert is tried.
     */
    public static final String INSERT_ATTEMPTS_PROPERTY = "insert-attempts";

    static final int LOAD_BATCH_SIZE = 1000;
    private static final long MICROS_PER_MILLI = 1000;

    private final AtomicLong lastVersion = new AtomicLong();

    private ILogger logger;
    private Properties properties;
    private String mapName;
    private ClientSource clientSource;
    private HecatonInstance instance;
    private TableName tableName;
    private String typeName;
    private boolean singleColumnAsValue;
    private boolean loadAllKeys;
    private boolean sequentialConsistency;
    private int insertAttempts;
    private Map<String, String> insertSettings;

    private volatile State state;
    private ClientSource.Lease lease;

    @Override
    public void init(HecatonInstance instance, Properties properties, String mapName) {
        this.instance = instance;
        this.properties = properties;
        this.mapName = mapName;
        this.logger = Logger.getLogger(ClickHouseMapStore.class);
        String dataConnection = properties.getProperty(DATA_CONNECTION_REF_PROPERTY);
        if (dataConnection != null) {
            clientSource = ClientSource.of(dataConnectionRef(dataConnection));
        } else if (properties.getProperty(ClickHouseDataConnection.ENDPOINTS_PROPERTY) != null) {
            Properties connectionProperties = new Properties();
            connectionProperties.putAll(properties);
            clientSource = ClientSource.of(() -> ClickHouseDataConnection.createClient(
                    "the map store of " + mapName, connectionProperties));
        } else {
            throw new HecatonException("The ClickHouse map store of map '" + mapName + "' needs the property '"
                    + DATA_CONNECTION_REF_PROPERTY + "' or '" + ClickHouseDataConnection.ENDPOINTS_PROPERTY + "'");
        }
        tableName = TableName.parse(properties.getProperty(EXTERNAL_NAME_PROPERTY, mapName));
        typeName = properties.getProperty(TYPE_NAME_PROPERTY, mapName);
        singleColumnAsValue = Boolean.parseBoolean(properties.getProperty(SINGLE_COLUMN_AS_VALUE_PROPERTY, "false"));
        loadAllKeys = Boolean.parseBoolean(properties.getProperty(LOAD_ALL_KEYS_PROPERTY, "true"));
        insertAttempts = Integer.parseInt(properties.getProperty(INSERT_ATTEMPTS_PROPERTY, "3"));
        insertSettings = new HashMap<>();
        String quorum = properties.getProperty(INSERT_QUORUM_PROPERTY);
        if (quorum != null) {
            insertSettings.put("insert_quorum", quorum);
        }
        sequentialConsistency = Boolean.parseBoolean(
                properties.getProperty(SELECT_SEQUENTIAL_CONSISTENCY_PROPERTY, "false"));
    }

    /**
     * Connects and checks the table on first use, so that the member starts even when
     * ClickHouse is not reachable; a failed check is repeated on the next use.
     */
    private State state() {
        State current = state;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            if (state == null) {
                ClientSource.Lease newLease = clientSource.open(
                        Util.getNodeEngine(instance).getDataConnectionService());
                try {
                    state = new State(newLease.client(), TableMetadata.read(newLease.client(), tableName));
                    lease = newLease;
                } catch (RuntimeException e) {
                    newLease.close();
                    throw e;
                }
            }
            return state;
        }
    }

    @Override
    public void destroy() {
        synchronized (this) {
            state = null;
            if (lease != null) {
                lease.close();
                lease = null;
            }
        }
    }

    @Override
    public void store(K key, V value) {
        storeAll(Collections.singletonMap(key, value));
    }

    @Override
    public void storeAll(Map<K, V> map) {
        State s = state();
        List<Object[]> rows = new ArrayList<>(map.size());
        for (Map.Entry<K, V> entry : map.entrySet()) {
            rows.add(s.fullRows.convert(s.row(entry.getKey(), entry.getValue(), nextVersion())));
        }
        s.fullRows.insert(s.client, rows);
    }

    @Override
    public void delete(K key) {
        deleteAll(Collections.singletonList(key));
    }

    @Override
    public void deleteAll(Collection<K> keys) {
        State s = state();
        List<Object[]> rows = new ArrayList<>(keys.size());
        for (K key : keys) {
            rows.add(s.deletions.convert(new Object[]{key, nextVersion(), 1}));
        }
        s.deletions.insert(s.client, rows);
    }

    @Override
    public V load(K key) {
        return loadAll(Collections.singletonList(key)).get(key);
    }

    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        State s = state();
        Map<K, V> result = new HashMap<>();
        List<K> batch = new ArrayList<>(Math.min(keys.size(), LOAD_BATCH_SIZE));
        for (K key : keys) {
            batch.add(key);
            if (batch.size() == LOAD_BATCH_SIZE) {
                s.load(batch, result);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            s.load(batch, result);
        }
        return result;
    }

    /**
     * Returns the keys of the table that are not deleted, read as they are iterated. The
     * iterable is {@link Closeable}: closing it ends the read.
     */
    @Override
    public Iterable<K> loadAllKeys() {
        if (!loadAllKeys) {
            return Collections.emptyList();
        }
        return state().loadAllKeys();
    }

    private QuerySettings querySettings() {
        QuerySettings settings = new QuerySettings();
        if (sequentialConsistency) {
            settings.serverSetting("select_sequential_consistency", "1");
        }
        return settings;
    }

    private long nextVersion() {
        long now = System.currentTimeMillis() * MICROS_PER_MILLI;
        return lastVersion.accumulateAndGet(now, (last, time) -> Math.max(last + 1, time));
    }

    /**
     * The connection and the columns, known once the table has been checked.
     */
    private final class State {

        final Client client;
        final ColumnType id;
        final List<ColumnType> values = new ArrayList<>();
        final RowInserter fullRows;
        final RowInserter deletions;
        final String selectSql;

        State(Client client, TableMetadata table) {
            this.client = client;
            String idColumn = properties.getProperty(ID_COLUMN_PROPERTY, "id");
            String versionColumn = properties.getProperty(VERSION_COLUMN_PROPERTY, "version");
            String isDeletedColumn = properties.getProperty(IS_DELETED_COLUMN_PROPERTY, "is_deleted");
            checkEngine(table, idColumn, versionColumn, isDeletedColumn);
            id = table.column(idColumn);
            ColumnType version = table.column(versionColumn);
            ColumnType isDeleted = table.column(isDeletedColumn);
            List<String> valueColumns = valueColumns(table, idColumn, versionColumn, isDeletedColumn);
            for (String column : valueColumns) {
                values.add(table.column(column));
            }
            if (singleColumnAsValue && values.size() != 1) {
                throw new HecatonException("The ClickHouse map store of map '" + mapName + "' has '"
                        + SINGLE_COLUMN_AS_VALUE_PROPERTY + "' set, but " + values.size() + " value columns: "
                        + valueColumns);
            }
            List<ColumnType> all = new ArrayList<>(List.of(id));
            all.addAll(values);
            all.addAll(List.of(version, isDeleted));
            fullRows = new RowInserter(table.name(), all, insertSettings, insertAttempts);
            deletions = new RowInserter(table.name(), List.of(id, version, isDeleted), insertSettings, insertAttempts);
            selectSql = selectSql(table.name(), isDeleted);
            logger.fine("ClickHouse map store of map '" + mapName + "' uses " + table.name() + ", key " + idColumn
                    + ", values " + valueColumns);
        }

        private List<String> valueColumns(TableMetadata table, String idColumn, String versionColumn,
                                          String isDeletedColumn) {
            String columns = properties.getProperty(COLUMNS_PROPERTY);
            if (columns != null) {
                return stream(columns.split(",")).map(String::strip).filter(c -> !c.isEmpty()).toList();
            }
            return table.columns().keySet().stream()
                        .filter(c -> !c.equals(idColumn) && !c.equals(versionColumn) && !c.equals(isDeletedColumn))
                        .toList();
        }

        private String selectSql(TableName table, ColumnType isDeleted) {
            StringBuilder select = new StringBuilder("SELECT ").append(TableName.quote(id.name()));
            for (ColumnType value : values) {
                select.append(", ").append(TableName.quote(value.name()));
            }
            return select.append(" FROM ").append(table.sql()).append(" FINAL WHERE ")
                         .append(TableName.quote(isDeleted.name())).append(" = 0").toString();
        }

        private void checkEngine(TableMetadata table, String idColumn, String versionColumn, String isDeletedColumn) {
            String problem = null;
            if (!table.engine().endsWith("ReplacingMergeTree")) {
                problem = "its engine is " + table.engine();
            } else if (!table.engineArguments().equals(List.of(versionColumn, isDeletedColumn))) {
                problem = "its engine's columns are " + table.engineArguments();
            } else if (!table.sortingKey().equals(idColumn)) {
                problem = "it is ordered by '" + table.sortingKey() + "'";
            }
            if (problem != null) {
                throw new HecatonException("The ClickHouse map store of map '" + mapName + "' needs a table with "
                        + "the engine ReplacingMergeTree(" + versionColumn + ", " + isDeletedColumn + ") or "
                        + "ReplicatedReplacingMergeTree(..., " + versionColumn + ", " + isDeletedColumn
                        + "), ordered by '" + idColumn + "' alone, but " + table.name() + " does not: " + problem);
            }
        }

        Object[] row(K key, V value, long version) {
            Object[] row = new Object[values.size() + 3];
            row[0] = key;
            if (singleColumnAsValue) {
                row[1] = value;
            } else if (value instanceof GenericRecord record) {
                for (int i = 0; i < values.size(); i++) {
                    row[i + 1] = ColumnType.getField(record, values.get(i).name());
                }
            } else {
                throw new HecatonException("The ClickHouse map store of map '" + mapName + "' stores GenericRecord "
                        + "values, but got a " + (value == null ? null : value.getClass().getName()) + "; set '"
                        + SINGLE_COLUMN_AS_VALUE_PROPERTY + "' to store a single column");
            }
            row[row.length - 2] = version;
            row[row.length - 1] = 0;
            return row;
        }

        @SuppressWarnings("unchecked")
        void load(List<K> keys, Map<K, V> result) {
            // keys as ClickHouse returns them, to find the requested key object of each row
            Map<Object, K> requested = new HashMap<>();
            Map<String, Object> parameters = new HashMap<>();
            StringBuilder sql = new StringBuilder(selectSql).append(" AND ").append(TableName.quote(id.name()))
                                                            .append(" IN (");
            for (int i = 0; i < keys.size(); i++) {
                K key = keys.get(i);
                Object converted = id.toClickHouse(key);
                requested.put(lookupKey(id.fromClickHouse(converted)), key);
                parameters.put("k" + i, id.toParameter(converted));
                sql.append(i == 0 ? "" : ", ").append("{k").append(i).append(':').append(id.parameterType()).append('}');
            }
            sql.append(')');
            try (QueryResponse response = client.query(sql.toString(), parameters, querySettings()).get();
                 ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
                while (reader.hasNext()) {
                    reader.next();
                    K key = requested.get(lookupKey(id.fromClickHouse(reader.readValue(1))));
                    if (key != null) {
                        result.put(key, (V) value(reader));
                    }
                }
            } catch (Exception e) {
                throw failure("load " + keys.size() + " keys from", e);
            }
        }

        /**
         * Returns a form of a key that equals the key ClickHouse returns for it: decimals without
         * their scale, times as instants.
         */
        private Object lookupKey(Object key) {
            if (key instanceof BigDecimal decimal) {
                return decimal.signum() == 0 ? BigDecimal.ZERO : decimal.stripTrailingZeros();
            } else if (key instanceof OffsetDateTime time) {
                return time.toInstant();
            }
            return key;
        }

        private Object value(ClickHouseBinaryFormatReader reader) {
            if (singleColumnAsValue) {
                return values.get(0).fromClickHouse(reader.readValue(2));
            }
            GenericRecordBuilder builder = GenericRecordBuilder.compact(typeName);
            for (int i = 0; i < values.size(); i++) {
                ColumnType column = values.get(i);
                column.setField(builder, column.name(), column.fromClickHouse(reader.readValue(i + 2)));
            }
            return builder.build();
        }

        Iterable<K> loadAllKeys() {
            String sql = "SELECT " + TableName.quote(id.name()) + selectSql.substring(selectSql.indexOf(" FROM "));
            try {
                QueryResponse response = client.query(sql, querySettings()).get();
                return new KeyIterable(response, client.newBinaryFormatReader(response));
            } catch (Exception e) {
                throw failure("load the keys from", e);
            }
        }

        private HecatonException failure(String action, Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return new HecatonException("The ClickHouse map store of map '" + mapName + "' could not " + action + " "
                    + tableName + ": " + e.getMessage(), e);
        }

        /**
         * The keys of the table, read as they are iterated.
         */
        private final class KeyIterable implements Iterable<K>, Closeable {

            private final QueryResponse response;
            private final ClickHouseBinaryFormatReader reader;
            private boolean iterated;

            KeyIterable(QueryResponse response, ClickHouseBinaryFormatReader reader) {
                this.response = response;
                this.reader = reader;
            }

            @Override
            public Iterator<K> iterator() {
                if (iterated) {
                    throw new IllegalStateException("The keys can be iterated only once");
                }
                iterated = true;
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        boolean hasNext = reader.hasNext();
                        if (!hasNext) {
                            close();
                        }
                        return hasNext;
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public K next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        reader.next();
                        return (K) id.fromClickHouse(reader.readValue(1));
                    }
                };
            }

            @Override
            public void close() {
                try {
                    reader.close();
                    response.close();
                } catch (Exception e) {
                    logger.fine("Closing the key query of the ClickHouse map store of map '" + mapName + "'", e);
                }
            }
        }
    }
}
