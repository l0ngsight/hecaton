# ClickHouse

The module `hecaton-jet-clickhouse` (in the full distribution's `lib/`) connects Hecaton to
[ClickHouse](https://clickhouse.com/):

- **`ClickHouseMapStore`** keeps a map's entries in a ClickHouse table;
- **`ClickHouseSources`** and **`ClickHouseSinks`** read and write tables in Jet pipelines;
- the **SQL connector** `ClickHouse` maps tables for `SELECT`, `INSERT INTO` and `SINK INTO`;
- **`ClickHouseDataConnection`** configures the connection once for all of them.

It is a Hecaton connector; Hazelcast has none for ClickHouse. It talks to ClickHouse's HTTP
interface through the official ClickHouse Java client (`com.clickhouse:client-v2`) and works with
ClickHouse 23.2 or later; the tests run against 25.8.

```xml
<dependency>
    <groupId>uk.org.hecaton.jet</groupId>
    <artifactId>hecaton-jet-clickhouse</artifactId>
    <version>${hecaton.version}</version>
</dependency>
```

## Data connection

```java
Config config = new Config();
config.addDataConnectionConfig(new DataConnectionConfig("clickhouse")
        .setType("ClickHouse")
        .setProperty("endpoints", "http://clickhouse-1:8123,http://clickhouse-2:8123")
        .setProperty("username", "hecaton")
        .setProperty("password", "...")
        .setProperty("database", "shop"));
```

```yaml
hecaton:
  data-connection:
    clickhouse:
      type: ClickHouse
      shared: true
      properties:
        endpoints: http://clickhouse-1:8123,http://clickhouse-2:8123
        username: hecaton
        password: ...
        database: shop
```

| Property | Default | Meaning |
|---|---|---|
| `endpoints` | (required) | HTTP(S) URLs of the servers, separated by commas; each request goes to one of them |
| `username`, `password` | `default`, empty | Credentials |
| `database` | the user's default | Database of tables named without one |
| `client.<option>` | | Any option of the ClickHouse Java client, for example `client.socket_timeout` or `client.ssl_mode` |

A shared data connection (the default) has one client for all its users; one that is not shared
creates a client for each processor or map store.

## Map store

ClickHouse does not update rows in place, so the map store needs a table of the
`ReplacingMergeTree` family with a version and an `is_deleted` column, ordered by the key alone:

```sql
CREATE TABLE shop.people ON CLUSTER my_cluster (
    id         Int64,
    name       String,
    age        Nullable(Int32),
    version    UInt64,
    is_deleted UInt8
) ENGINE = ReplicatedReplacingMergeTree(version, is_deleted)
ORDER BY id
```

Every store inserts a row with a higher `version`; every delete inserts a row with
`is_deleted = 1`. ClickHouse keeps the latest row of each key when it merges parts, and the map
store reads with `FINAL`, which returns only those rows, so a read always sees the latest store
or delete. `ReplacingMergeTree` (not replicated) works the same on a single server.

```java
Properties properties = new Properties();
properties.setProperty("data-connection-ref", "clickhouse");
properties.setProperty("external-name", "shop.people");
config.addMapConfig(new MapConfig("people").setMapStoreConfig(new MapStoreConfig()
        .setEnabled(true)
        .setClassName("uk.org.hecaton.jet.clickhouse.ClickHouseMapStore")
        .setProperties(properties)));

IMap<Long, GenericRecord> people = member.getMap("people");
people.put(1L, GenericRecordBuilder.compact("people")
        .setString("name", "Ann")
        .setNullableInt32("age", 31)
        .build());
```

Values are compact `GenericRecord`s with a field per value column, as with Hazelcast's
`GenericMapStore`: a client that has a class with the same compact type name reads them as that
class. With `single-column-as-value`, the value is the value of the only value column.

| Property | Default | Meaning |
|---|---|---|
| `data-connection-ref` | | The ClickHouse data connection. Instead, the map store can take the data connection's properties (`endpoints`, ...) for a client of its own |
| `external-name` | the map name | The table, `table` or `database.table` |
| `id-column` | `id` | The key column, which must be the table's whole `ORDER BY` |
| `version-column`, `is-deleted-column` | `version`, `is_deleted` | The engine's columns |
| `columns` | all other columns | Value columns, separated by commas |
| `type-name` | the map name | Type name of the compact records |
| `single-column-as-value` | `false` | The value is the only value column |
| `load-all-keys` | `true` | Load the table's keys when the map is first used (keys are read as a stream) |
| `insert-quorum` | not set | `insert_quorum` of the inserts, for example `auto` for a majority of the replicas |
| `select-sequential-consistency` | `false` | Read only from replicas that have every quorum insert |
| `insert-attempts` | 3 | Tries per insert; a retried insert keeps its deduplication token |

The map store connects and checks the table on first use, so members start while ClickHouse is
down; a table with another engine, other engine columns or another `ORDER BY` is refused with a
message saying what to change.

### Replicas and consistency

By default an insert succeeds once one replica has it, and another replica receives it within
moments. A member that reads through another replica in that time reads the previous value. For
reads that always see the latest write, set `insert-quorum` (for example to the number of
replicas, or `auto`) and `select-sequential-consistency=true`: inserts then wait for the quorum,
and reads go only to replicas that have them.

Versions come from the member's clock, in microseconds, and increase on each member. An entry is
stored by the member that owns its partition; if a partition moves to a member whose clock is
behind, a store right after the move can lose against the previous one. Keep the members' clocks
synchronized (NTP), as for any time-based ordering.

Sharded tables (`Distributed` over several shards) are not supported: `FINAL` only collapses the
rows of a key within one shard.

## Jet sources

```java
// a table, read in parallel by every member: each processor reads the rows whose
// split column hashes to it
BatchSource<Person> people = ClickHouseSources
        .builder("people", row -> new Person(row.getLong("id"), row.getString("name")))
        .dataConnectionRef(dataConnectionRef("clickhouse"))
        .table("shop.people")
        .columns("id", "name")
        .filter("is_deleted = 0")
        .useFinal(true)
        .splitColumn("id")
        .build();

// a query, run by one processor
BatchSource<Tuple2<String, Long>> counts = ClickHouseSources.query("counts",
        dataConnectionRef("clickhouse"),
        "SELECT kind, count() AS c FROM shop.events GROUP BY kind",
        row -> tuple2(row.getString("kind"), row.getLong("c")));
```

The map function reads the current row from the client's `ClickHouseBinaryFormatReader`, by
column name or position, as with JDBC's `ResultSet`.

## Jet sinks

```java
Sink<Person> sink = ClickHouseSinks.insert("people", dataConnectionRef("clickhouse"),
        "shop.people", "id, name, version, is_deleted",
        p -> new Object[]{p.id(), p.name(), p.version(), 0});
```

`ClickHouseSinks.builder` also sets the batch size (10,000 rows), insert settings such as
`insert_quorum`, the attempts per batch and the local parallelism. Each batch is one insert. A
failed insert is retried with the same `insert_deduplication_token`, so a batch that did reach a
replicated table (or a table with `non_replicated_deduplication_window`) is not inserted twice.
After a job restarts, the sink can insert rows again (at-least-once); a `ReplacingMergeTree`
table with a version column collapses them.

## SQL

```sql
CREATE DATA CONNECTION clickhouse TYPE ClickHouse SHARED
OPTIONS ('endpoints' = 'http://clickhouse-1:8123', 'username' = 'hecaton', 'password' = '...');

-- the columns come from the table
CREATE MAPPING people EXTERNAL NAME shop.people DATA CONNECTION clickhouse;

-- or name the fields, with other names or types
CREATE MAPPING people_names EXTERNAL NAME shop.people (
    key BIGINT EXTERNAL NAME id,
    label VARCHAR EXTERNAL NAME name
) DATA CONNECTION clickhouse;

SELECT name, age FROM people WHERE id >= ? AND age IS NOT NULL;
INSERT INTO people SELECT v, 'generated', 30, 1, 0 FROM TABLE(generate_series(1, 1000));
SINK INTO people VALUES (?, ?, ?, ?, ?);
```

A mapping without fields takes every column of a supported type (see [Types](#types)); columns
of other types, such as arrays, are left out. A field's type can differ from its column's when
the column's type converts to it.

A `SELECT` sends to ClickHouse the parts of the `WHERE` clause that mean the same there:
comparisons of columns with literals and parameters, `AND`, `OR`, `NOT`, `IS [NOT] NULL`. Values
are sent as query parameters. Ordering comparisons of strings stay in Hecaton (ClickHouse orders
strings by their UTF-8 bytes), as do functions and casts. The query reads only the columns it
uses, and Hecaton evaluates the whole condition on the rows it receives, so results follow
Hecaton's SQL whatever ClickHouse returns.

`INSERT INTO` and `SINK INTO` insert batches of rows: each insert holds what the processor has
received, up to `insertBatchSize` rows, and a failed insert is retried with the same
deduplication token. There is no `UPDATE` or `DELETE`: for a table whose rows change, insert new
versions into a `ReplacingMergeTree` table and read it with `useFinal`.

| Option | Default | Meaning |
|---|---|---|
| `useFinal` | `false` | Read with `FINAL`: only the latest row of each key of a `ReplacingMergeTree` table |
| `splitColumn` | not set | Read in parallel on every member, each processor reading the rows whose column hashes to it; without it, one processor reads |
| `preferredLocalParallelism` | 2 | Processors per member of a split read |
| `insertBatchSize` | 10000 | Largest number of rows in one insert |
| `insertQuorum` | not set | `insert_quorum` of the inserts |

## Types

| ClickHouse | Java (map values, keys) | Compact field | SQL |
|---|---|---|---|
| `Bool` | `Boolean` | `BOOLEAN` | `BOOLEAN` |
| `Int8` | `Byte` | `INT8` | `TINYINT` |
| `UInt8`, `Int16` | `Short` | `INT16` | `SMALLINT` |
| `UInt16`, `Int32` | `Integer` | `INT32` | `INTEGER` |
| `UInt32`, `Int64` | `Long` | `INT64` | `BIGINT` |
| `UInt64`, `(U)Int128`, `(U)Int256`, `Decimal*` | `BigDecimal` | `DECIMAL` | `DECIMAL` |
| `Float32`, `Float64` | `Float`, `Double` | `FLOAT32`, `FLOAT64` | `REAL`, `DOUBLE` |
| `String`, `FixedString`, `Enum8/16`, `UUID`, `IPv4`, `IPv6` | `String` | `STRING` | `VARCHAR` |
| `Date`, `Date32` | `LocalDate` | `DATE` | `DATE` |
| `DateTime`, `DateTime64` | `OffsetDateTime` | `TIMESTAMP_WITH_TIMEZONE` | `TIMESTAMP WITH TIME ZONE` |

`Nullable(T)` maps to the nullable compact kinds, and `LowCardinality(T)` like `T`. When storing,
a value of another numeric type is converted (an `Integer` key for an `Int64` column, for
example). Arrays, maps, tuples and the other types are not supported by the map store and SQL;
the Jet source and sink give access to every type through the ClickHouse client.
