# Hecaton in Rust — Reimplementation Plan

| | |
|---|---|
| Status | Draft v0.1, for discussion |
| Scope | A Rust implementation of the Hecaton **member**, wire-compatible with the Hazelcast 5.x client protocol; a Java client that stays; an **embedded mode** that runs the Rust member inside a JVM |
| Baseline | Hecaton `main` (Hazelcast CE 5.7.0 lineage), see [`SPEC.md`](SPEC.md) |
| Effort unit | engineer-weeks (ew), order of magnitude |
| New repository | to be created; layout in §12 |

Section references of the form "spec §n" point to [`SPEC.md`](SPEC.md). Decisions are
numbered R1…; features keep the spec's F numbers where they exist.

---

## 1. Summary

Hecaton today is 845k lines of Java in the core module alone (6,261 main files), plus SQL
(706 files, Hazelcast Community License), Jet connectors, Spring integration and the member
modules written for Hecaton (HD, persistence, tiered, vector). Member and client share one
jar; the client is about three quarters of the core's classes by reachability
(`IMPLEMENTATION_PLAN.md`, slim client jar notes).

This plan rewrites the **member** in Rust and keeps the **Java client**:

1. **The Rust member speaks the Hazelcast 5.x client protocol byte for byte** (`CP2`
   framing, 477 generated codecs, 104 error codes, Compact schema distribution, backup acks,
   cluster and partition views), so the official Hazelcast clients for Java, .NET, Python,
   Node.js, Go and C++ and Hecaton's own Java client connect unchanged (spec G3). The
   existing `compat-tests/` suite is the acceptance test.
2. **The Java client stays Java.** The current `uk.org.hecaton` client code (1,380 files
   under `client/` plus serialization, config and the public API) is carved out of the
   core into a real client-only artifact `hecaton-client`, including the JCache
   (`javax.cache`) client provider. It is the same code applications use today.
3. **Embedded mode is kept** (§7). `Hecaton.newHecatonInstance(config)` in a Java 25
   process loads the Rust member as a native library through the FFM API and talks to it
   in-process. Because the host JVM is right there, Java user code (entry processors,
   map stores, executors, custom predicates) runs in embedded mode without a second runtime.
4. **The member protocol is new and Rust-only.** Spec NG1 already forbids mixed clusters, so
   nothing on the member-to-member wire has to be preserved. Java Hecaton clusters migrate
   with the existing client-based data copy (`hecaton-migrate data`).
5. **Jet is not reimplemented.** Jet pipelines are Java lambdas executed in a JVM cluster;
   that is a different project. Batch SQL is reimplemented on Apache DataFusion; streaming
   SQL (which runs on Jet) is out of scope (§5, R8).

The estimate for a 1.0 with IMap, ICache, the collections, CP Subsystem, security/TLS,
persistence, WAN and batch SQL, but without Jet, is **120–160 ew**: two to three engineers
for 12–18 months (§10).

### 1.1 Goals

- RG1: Every official Hazelcast 5.5–5.7 client and the Hecaton Java client pass
  `compat-tests/` against a Rust cluster, with no client-side change beyond addresses.
- RG2: Data structures that carry no Java code (IMap, ICache, MultiMap, Queue, List, Set,
  Ringbuffer, Topic, ReliableTopic, ReplicatedMap, PNCounter, FlakeIdGenerator,
  CardinalityEstimator, all CP structures, CPMap, transactions) reach feature parity with
  Hecaton's Java member, including listeners, near-cache invalidation, indexes, built-in
  predicates/aggregators/projections, TTL/eviction and backups.
- RG3: Features Hecaton added to CE keep working: TLS (F1), authentication and permissions
  (F2, F3), CP Subsystem and CPMap (F7, F8), persistence (F10), WAN (F13), client failover
  (F15), audit log (F4).
- RG4: Lower and flatter latency than the Java member, no GC pauses, and a much smaller
  memory footprint for the same data (all data is already "off-heap"; F11 HD memory becomes
  the only mode).
- RG5: A single static binary and a container image under 50 MB; start in under a second.
- RG6: One Java API for both deployment modes: client/server (`HecatonClient`) and
  embedded (`Hecaton.newHecatonInstance`), as today.

### 1.2 Non-goals

- RN1: Running Java user code on a **standalone** Rust member without the optional JVM
  runtime module (§5.4). Standalone members reject Java-only operations with a precise
  error that clients already understand (`UnsupportedOperationException`, error code 61).
- RN2: Jet (stream processing, pipelines, jobs) and streaming SQL. Applications that need
  Jet stay on the Java member, which keeps a maintenance line (R12).
- RN3: Member-protocol compatibility with Java Hecaton or Hazelcast members (NG1).
- RN4: Reading Java Hecaton persistence directories (same stance as spec NG2).
- RN5: Management Center (spec NG3), JMX. Metrics are Prometheus/OpenMetrics and REST.
- RN6: Cloud-provider discovery other than Kubernetes (spec NG6).

---

## 2. What has to be compatible, exactly

Facts from the current source tree; these define the contract the Rust member implements.

### 2.1 Connection and handshake

| Item | Value | Source |
|---|---|---|
| Protocol preamble | 3 bytes: `CP2` (client), `HTTP` (REST), `Memcached`; member protocol is `HCC`, Hazelcast's `HZC` is refused | `internal/nio/Protocols.java` |
| Frame | `int` length, `short` flags, content; flags: begin/end fragment (bits 15, 14), final (13), begin/end data structure (12, 11), null (10), event (9), backup-aware (8), backup-event (7) | `client/impl/protocol/ClientMessage.java` |
| Initial frame | message type `int`, correlation id `long`, then partition id `int` (request/event) or backup acks `byte` (response) | same |
| Message type ids | e.g. `Map.AddEntryListener` request 71936, response 71937, event 71938; ids come from the protocol definitions | generated codecs |
| Authentication request | cluster name, username, password, client UUID, client type, serialization version (1), client version string, client name, labels, routing mode byte (5.5+), CP direct-to-leader flag (5.5+); older clients send shorter initial frames, the decoder checks the frame length | `ClientAuthenticationCodec` |
| Authentication response | status (0 ok, 1 credentials failed, 2 serialization mismatch, 3 not allowed), member address, member UUID, serialization version, server version, partition count, cluster id, failover-supported, TPC ports and token (null = TPC off), member list version + member infos, partition list version + partition table, key-value pairs (member groups for `MULTI_MEMBER` routing) | same |
| Custom authentication | `ClientAuthenticationCustomCodec` carries token credentials as bytes | |
| Keep-alive | `Client.Ping`; `Client.Statistics` (client stats + Compact schema warm-up) | |
| Cluster views | `Client.AddClusterViewListener` events: member list (with version) and partition table (with version); `Client.AddCPGroupViewListener` for CP leader routing | |
| Backup acks | Requests set the backup-aware flag; response carries the backup count; each backup member sends a `Client.LocalBackupListener` event with the correlation id | |
| Fragmentation | Large responses are split into frames with begin/end fragment flags and a fragmentation id | |
| Errors | `ErrorsCodec`: list of `ErrorHolder` (code, class name, message, stack trace). 104 codes; clients map by code | `ClientProtocolErrorCodes` |
| Compact schemas | `Client.SendSchema` (replicate to all members before acking), `Client.FetchSchema`, `Client.SendAllSchemas` | `compact/schema/*` |
| TPC | Not supported: `tpcPorts`/`tpcToken` are null, clients fall back to classic | design/alto |

### 2.2 Serialization (`Data`)

`Data` = `int` partition hash + `int` type id + payload. Partition hash is MurmurHash3
x86-32 of the payload (or of the partition-key part for `PartitionAware` keys and
`@key#field` partitioning attributes); `partitionId = |hash| mod partitionCount`
(default 271). **Clients compute this themselves for routing**, so the Rust side must
produce identical results.

| Type id | Type | Rust handling |
|---|---|---|
| 0, −3…−23 | null, primitives, arrays, String (UTF-8 with length), UUID, Map.Entry | full |
| −1 | Portable (class definitions, versioned fields, nested) | full: needed for queries on Portable values |
| −2 | IdentifiedDataSerializable | **built-ins only**: predicates (factory −20), aggregators (−29), projections (−30), SQL types (−38), JSON (−39), schema (−42), config (−31, dynamic config), partitioning/`PartitionAware` helpers; user IDS values are opaque bytes |
| −55, −56 | Compact (schema id + offsets; −56 carries the schema inline) | full, including `GenericRecord` views for queries, indexes and SQL |
| −130 | `HecatonJsonValue` | full (JSON path queries, indexes) |
| −100, −101 | Java `Serializable`/`Externalizable` | **opaque**; stored and returned, not queryable; rejected where a value must be evaluated unless the JVM runtime is present |

Type ids above are from `SerializationConstants`; the generated Rust constants come from the
same file, checked by a test against the Java oracle (§9.1).

### 2.3 Server-side surface by codec family

477 codecs. Grouped by prefix with the proposed phase (§10):

| Family | Codecs | Phase | Notes |
|---|---|---|---|
| Client | 23 | M1 | auth, views, ping, stats, schemas, proxies, listeners |
| Map | 73 | M1–M3 | 7 of them (`ExecuteOnKey`, `ExecuteOnAllKeys`, `ExecuteWithPredicate`, `SubmitToKey`, `AddInterceptor`, `RemoveInterceptor`, `LoadAll`/`LoadGivenKeys` map-store paths) need Java user code → JVM runtime or embedded mode |
| Cache (ICache/JCache) | 33 | M3 | expiry policies, listeners, event journal, `invoke` needs user code |
| Transactional*, Transaction, XATransaction | 45 | M3 (XA M11) | two-phase client transactions |
| Multi(Map), List, Set, Queue, Ringbuffer, Topic | 92 | M3 | |
| Replicated(Map) | 21 | M3 | |
| Atomic*, Fenced*, Semaphore, Count(DownLatch), CPGroup/CPSession/CPSubsystem, CPMap | ~45 | M5 | |
| Dynamic* (dynamic config) | 20 | M3 | config IDS (−31) decoding |
| Scheduled/Durable/Executor | 30 | JVM runtime only | Java callables |
| Continuous(QueryCache) | 6 | M11 | |
| Cardinality, PNCounter, FlakeId | ~8 | M3 | |
| Sql | 6 | M8 | `Execute`, `Fetch`, `Close`, `MappingDdl` |
| Vector | 11 | M11 | |
| Jet | 20 | never (RN2) | respond `UnsupportedOperation` |
| MC* (Management Center) | ~40 | never (RN5) | except the few `hc-cli` uses |

### 2.4 Text protocols and operations surface

REST (`/hecaton/rest/...`, `/hecaton/health/...`, `/hazelcast/...` alias), Memcache,
Prometheus metrics, the `hc`, `hc-start`, `hc-stop`, `hc-cli`, `hc-cluster-admin`,
`hc-cluster-cp-admin`, `hc-healthcheck` scripts, and the config surface
(`hecaton.xml`/`hecaton.yaml`, `HC_*` env overrides, `-Dhc.<path>` → `--set hc.<path>`).

---

## 3. Decisions

| # | Decision | Rationale |
|---|---|---|
| **R1** | Member in Rust; **Java client kept**, carved out as a client-only module | The protocol contract is the client's; the Java client is mature and already what applications use. A Rust client exists too (shared codec crate) for Rust applications and for the `hc-cli` tool |
| **R2** | Codecs **generated** from the Apache-2.0 `hazelcast/hazelcast-client-protocol` YAML definitions, pinned to the tag that produced the 5.7.0 Java codecs | Hand-writing 477 codecs is error-prone; the YAML plus a Rust template (Jinja → `askama`/`tera` or a small Python generator like upstream's) is a few days of work and stays in sync by construction. The Java codecs in this repo are the oracle for byte-exact tests |
| **R3** | New member protocol: length-prefixed frames, versioned messages with `postcard` or hand-rolled encoding, one multiplexed connection per member pair plus a separate stream for migrations and WAN | NG1 frees us; IDS-based operations would drag the whole Java serialization model into the member core |
| **R4** | Runtime: `tokio` for I/O and generic work; **partition shards** own partitions and run their operations single-threaded, actor style (messages over bounded channels), mirroring Hazelcast's partition-thread model that the backup/ordering semantics depend on | Clients and the protocol assume per-partition linearizable ordering and partition-id routing. Shards also avoid locks on the hot path. Later: pin shards to cores and use `io_uring` (what the Java TPC engine tried to be) |
| **R5** | Storage: one in-memory format. `in-memory-format: BINARY/OBJECT/NATIVE` are all accepted; values are stored as `Data` bytes in a Rust allocator with per-map accounting, and `OBJECT` additionally caches decoded query views for Compact/Portable/JSON values | There is no heap to escape: HD (F11) is the default. `native-memory` config maps onto a global memory budget and eviction |
| **R6** | Persistence (F10): own per-partition **write-ahead log + snapshot** files, encrypted at rest with `aes-gcm` behind the existing config | A data grid holds everything in memory; restart needs a durable log, not a KV engine. Avoids a C++ dependency. RocksDB/`fjall` are candidates for Tiered Storage (F12) later |
| **R7** | CP Subsystem (F7): Raft via **`openraft`** with Hecaton's group, session, lock, semaphore, latch and CPMap state machines on top; CP persistence (F9) through its storage trait | Writing a correct Raft is a project in itself; `openraft` is mature, async and storage-agnostic. Fallback: an in-house Raft if `openraft`'s membership-change model fights CP group semantics (spike in M0) |
| **R8** | SQL: batch SQL on **Apache DataFusion** (`sqlparser-rs` with a Hazelcast dialect layer); no streaming SQL, no Jet | The HCL-licensed Java engine is Calcite-bound. A clean-room Rust engine written from public behaviour and the `docs/design/sql` design notes (not from the HCL sources) ships under Apache 2.0, removing the licence split (spec §11.2). Needs legal confirmation before M8 starts |
| **R9** | Security: TLS with `rustls` (F1); realms simple/username-password, token, TLS client certificate (P0); LDAP via `ldap3` (P1); Kerberos via `libgssapi`/`cross-krb5` (P2). JAAS custom login modules only in embedded mode or with the JVM runtime | |
| **R10** | Java user code: trait `UserCodeRuntime` with two implementations — **host JVM in embedded mode** (§7) and an optional **`hecaton-jvm` module** that loads `libjvm` into a standalone member through the `jni` crate | Entry processors, map stores, executors, custom predicates and interceptors are Java by definition. Standalone members without the module refuse them with a clear error |
| **R11** | Config: the same XML/YAML documents (`hecaton.xml`/`hecaton.yaml`, same XSD), parsed in Rust; unknown or Java-only elements (Jet, user-code-deployment, cloud discovery) fail with a targeted message or an INFO log per a documented table | Spec G4 carries over: an EE-style config file keeps working |
| **R12** | The Java member becomes a **maintenance line** (`java-member` branch, 1.x releases) until the Rust member reaches M9; the main Java repo then holds only the client, Spring integration and the embedded-mode shim | Users with Jet keep a supported product; the team does not maintain two members forever |
| **R13** | Licensing: everything Apache 2.0; clean-room rules of `CONTRIBUTING.md` extended: Rust code is written from the public protocol/behaviour and the Apache-2.0 Java sources, never from `hecaton-sql` or the HCL connectors | |
| **R14** | Toolchain: stable Rust, MSRV = current stable − 2, `cargo-deny`, `clippy` pedantic, `miri` on every crate with `unsafe`, `loom` for the shard and invocation state machines, `proptest`, `cargo-fuzz` on the frame decoder, `Data` decoder and Compact reader (clients are untrusted input) | |

---

## 4. Architecture

```
                       official HZ clients (Java/.NET/Py/Node/Go/C++)   Hecaton Java client   Rust client crate
                                    │                 CP2 (unchanged)      │                      │
┌───────────────────────────────────▼─────────────────────────────────────▼──────────────────────▼──┐
│ hecaton-member (binary)   or   libhecaton.so inside a JVM (embedded mode, §7)                      │
│                                                                                                    │
│  net: tokio listeners ─ protocol demux (CP2 / HCC / HTTP / Memcached) ─ rustls                      │
│  client-engine: message tasks (generated dispatch) · client registry · listeners · schema service   │
│  spi: invocation · operation routing · event service · proxy registry · backups · retries           │
│  partition shards (N):  ┌──────────┐ ┌──────────┐ ┌──────────┐    generic pool: cluster, CP,       │
│   partitions i ≡ k (mod N) │ map/cache│ │collections│ │ query/idx│    WAN, persistence I/O, SQL     │
│                          └──────────┘ └──────────┘ └──────────┘                                     │
│  cluster: join (tcp-ip, multicast, k8s) · heartbeats/φ-detector · cluster state · split-brain merge  │
│  partition table · migrations · anti-entropy · graceful shutdown                                    │
│  cp (openraft) · security · wan · persistence (WAL+snapshot) · sql (DataFusion) · metrics · rest     │
│  user-code runtime: host JVM upcalls (embedded) | hecaton-jvm (optional) | none (refuse)             │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 4.1 Request path (IMap.put from a smart client)

1. The client hashes the key, picks the partition owner from the partition table it got in
   the auth response, sends `Map.Put` with partition id and the backup-aware flag.
2. `net` decodes frames into a `ClientMessage`; `client-engine` looks up the message task
   by message type (generated `match`), checks permissions (F3), decodes parameters.
3. The task builds a `PutOperation` and hands it to `spi::invoke(partition_id, op)`. If
   this member owns the partition, the op goes to the shard's channel; otherwise it is
   forwarded over the member protocol (clients normally route correctly; forwarding covers
   migrations and unisocket clients).
4. The shard applies the op to the record store (TTL, eviction, listeners, near-cache
   invalidation, event journal, WAN queue, persistence WAL append), then issues backup ops
   to the backup replicas (sync backups awaited, async not).
5. The response frame carries the backup count; each backup replica sends the client a
   `LocalBackupListener` event when its backup is applied.

### 4.2 Threading and ordering

- One shard per `hc.operation.thread.count` (default: CPU count). A shard is a single
  task/thread owning `partitionCount / N` partitions, their record stores, indexes and
  listener registrations. No locks on partition data.
- Generic operations (cluster, CP, proxies, config) run on a tokio pool.
- Blocking operations (`IQueue.take`, `IMap.lock`, `ICountDownLatch.await`,
  `FencedLock`) are parked as futures keyed by partition and woken by the shard, like
  Hazelcast's `WaitNotifyKey`.
- Invocation timeouts, retries on `RetryableException`, call-id sequencing and
  back pressure follow the Java `InvocationFuture` rules so client-visible behaviour is the
  same.

### 4.3 Cluster

- Join: TCP/IP list, multicast, Kubernetes (DNS and API modes, auto-detect in a pod),
  Discovery SPI replaced by a small `Discovery` trait (static, DNS, Kubernetes; plugins as
  dynamic libraries are not planned).
- Master-elected membership with a monotonically increasing member-list version (clients
  depend on it), heartbeats, the φ-accrual failure detector, suspicion and explicit
  "member removed" events, lite members, member attributes, partition groups (host, zone,
  node-aware, custom).
- Partition table owned by the master, versioned (clients depend on it), 271 partitions,
  up to 6 backups, migrations with per-partition replica versions and anti-entropy,
  graceful shutdown handing partitions off, `ClusterState` (ACTIVE, NO_MIGRATION, FROZEN,
  PASSIVE, IN_TRANSITION), split-brain protection and merge with the built-in merge policies
  (`PutIfAbsent`, `LatestUpdate`, `HigherHits`, `Expiration`, ... by their existing class
  names).

---

## 5. Feature plan

### 5.1 Data structures (RG2)

| Structure | Rust phase | Notes |
|---|---|---|
| IMap | M1 core, M3 full | records, TTL/max-idle, eviction (LRU/LFU/RANDOM/NONE, size policies), listeners with built-in predicates, near-cache invalidation + metadata (sequence/UUID per partition), entry views, locks, `putAll`/`getAll` batching, event journal, `keySet/values/entrySet(predicate)`, paging predicates, aggregators and projections, merge policies, split-brain, WAN hooks, persistence hooks |
| Indexes | M3 | hash, sorted (B-tree), bitmap; on Compact, Portable, JSON and primitive values; `__key`, `this`, attribute paths with `[any]` |
| ICache (JCache) | M3 | expiry policies (created/accessed/modified/touched/eternal), `CacheEntryListener`s with filters, statistics, event journal, `putIfAbsent`/`replace` family, `invoke` only with user code |
| MultiMap | M3 | set and list collections, locks |
| IQueue | M3 | bounded, TTL, listeners, `QueueStore` only with user code |
| IList, ISet | M3 | |
| Ringbuffer | M3 | TTL, overflow policies, `readMany` with filter (built-in predicate) |
| ITopic, ReliableTopic | M3 | reliable topic is a client-side structure on a ringbuffer |
| ReplicatedMap | M3 | vector-clock-free "last writer wins" per partition as upstream, anti-entropy |
| PNCounter, FlakeIdGenerator, CardinalityEstimator | M3 | HyperLogLog++ with identical merge semantics |
| Transactions | M3 | `TransactionalMap/MultiMap/Queue/List/Set`, two-phase with prepare/commit logs; XA in M11 |
| CP: AtomicLong, AtomicReference, FencedLock, Semaphore, CountDownLatch, CP sessions, group management | M5 | |
| CPMap | M5 | |
| Vector collection | M11 | `hnsw_rs` or `usearch`; same config and codecs |
| IExecutorService, DurableExecutor, ScheduledExecutor | JVM runtime / embedded | Java callables |
| Query cache (continuous query) | M11 | |

### 5.2 Hecaton features

| Feature | Phase | Rust approach |
|---|---|---|
| F1 TLS (member, client, WAN, REST/Memcache, mTLS, cert hot-reload) | M4 | `rustls`, PEM and PKCS#12 keystores (`p12` via `openssl`-free crates), protocol and cipher selection per config |
| F2 Authentication | M4 | realms: simple, username/password (bcrypt/argon2 stored), token, TLS cert; LDAP M9; Kerberos M11 |
| F3 Authorization | M4 | same `PermissionConfig` model, principal/endpoint matching, all permission types; `SecurityInterceptor` only via user-code runtime |
| F4 Audit log | M9 | `tracing` sink with the same event type ids |
| F5 Socket interceptor | user-code runtime only | |
| F7–F9 CP | M5 (persistence M6) | `openraft` |
| F10 Persistence | M6 | per-partition WAL + snapshot, cluster metadata store, coordinated restart (full/partial start policies), hot backup directories, encryption at rest (M9) |
| F11 HD memory | M1 | the only mode (R5) |
| F12 Tiered Storage | M11 | on a KV engine |
| F13 WAN | M7 | batch publisher with queue-full policies and ack types, consumer, pause/stop/resume, dynamic add; sync and Merkle-tree consistency check in M9 |
| F14 Rolling upgrades | M9 | versioned member protocol from day one so N−1 → N works |
| F15 Client failover | M1 | `failoverSupported = true` in the auth response |
| F16 Dynamic config + persistence | M3 / M6 | |
| F17 User Code Namespaces | embedded / JVM runtime | |
| F18 Jet extras, F19 SQL catalog persistence | never / M8 | |
| F20 Vector | M11 | |
| F23 Load protection (immutable maps, back pressure, connection churn, sharded queues) | M3 | bounded channels and per-connection budgets make most of it natural |

### 5.3 SQL (R8)

Batch SQL over IMap and ReplicatedMap only, in M8:

- `sqlparser-rs` with a dialect layer for Hazelcast specifics (`__key`, `this`,
  `CREATE/DROP MAPPING`, `CREATE/DROP VIEW`, `CREATE/DROP INDEX`, `CREATE DATA CONNECTION`,
  `SINK INTO`, `SHOW MAPPINGS`, `EXPLAIN`, the `hecaton` catalog with `public` and
  `information_schema`).
- DataFusion logical plan + custom physical operators: partition-parallel scans with
  predicate push-down into the shard indexes, key-based point lookups, partition pruning.
- Hazelcast type system and functions mapping (`JSON_QUERY`, `JSON_VALUE`, `TO_ROW`,
  temporal types, `OBJECT` type), result paging through `Sql.Execute`/`Sql.Fetch` with the
  `SqlPage` column-oriented encoding, cursor timeouts, `SqlRowMetadata`.
- DML: `INSERT`/`SINK INTO`/`UPDATE`/`DELETE` on IMap.
- Not covered: streaming queries, Kafka/file/JDBC mappings (they are Jet connectors),
  `CREATE JOB`.

### 5.4 Java user code on standalone members (R10, optional `hecaton-jvm`)

- Standalone members load `libjvm` lazily when `hc.java-runtime.enabled=true`, with a
  class path of `hecaton-usercode.jar` (a small Java module built from today's Apache-2.0
  sources: serialization, predicates, entry-processor and map-store interfaces) plus the
  application jars.
- Calls go through the `jni` crate on dedicated attached threads with a bounded pool;
  the record store exposes a read/write view through callbacks so an `EntryProcessor`
  mutates the Rust record in place.
- Covers: entry processors, custom predicates/aggregators/projections, map store/loader
  (including the generic JDBC map store), queue store, executors, interceptors, custom
  merge policies, custom login modules.
- Does not cover Jet.

The same `UserCodeRuntime` trait is implemented by the host JVM in embedded mode (§7),
where there is nothing to load.

---

## 6. The Java client

### 6.1 Carve-out

The client is already a distinct code base inside the core (`client/`, 1,380 files; plus
`internal/serialization`, `config`, `core`, `query` API types, `cp` API, `cache` API,
`spring`). The slim-jar investigation (`tools/client-jar/reachability.py`) found the
blockers: shared serializer hooks, `HecatonInstanceFactory`, CP proxy helpers in
`RaftService`. The carve-out is:

1. Split the serializer hooks into client-side (API types, predicates, aggregators,
   projections, config) and member-side (operations) hooks; the client registers only the
   former.
2. Move member-only types the client references by name (`RaftService` helpers, proxy
   factories) behind small client-side interfaces.
3. Build `hecaton-client` as a real module with its own sources (Java 17 bytecode, runs
   on 17/21/25), checked by an ArchUnit rule: no dependency on member packages.
4. Keep `hecaton-spring` and `hecaton-spring-boot-autoconfiguration` for the client side
   (`HecatonClient` beans, `@Cacheable` via the JCache provider).
5. JCache: the client `CachingProvider` (`uk.org.hecaton.client.cache.impl.HecatonClientCachingProvider`)
   and the `ICache` proxies stay as they are.

### 6.2 Tests

Client tests today start in-process Java members (`TestHecatonFactory`) and a mock
client. They move to a **Rust member fixture**: a JUnit extension that starts `hecaton-member`
binaries (downloaded per version or built from the sibling repo) and exposes the same
factory API. The 641 client test files are migrated in groups; tests that assert on member
internals are dropped or rewritten as protocol-level assertions. `compat-tests/` keeps
running against both members during the transition.

### 6.3 Releases

`uk.org.hecaton:hecaton-client` keeps its coordinate. A client release is independent of
member releases; the compatibility matrix (client 5.x protocol against member versions) is
tested in CI.

---

## 7. Embedded mode: the Rust member inside a JVM

Hazelcast's embedded mode (`newHazelcastInstance()` in the application JVM) is widely used
and must survive the rewrite (RG6). The design:

### 7.1 Mechanics

- `hecaton-ffi` crate: a C ABI (`cbindgen` header) exported from `libhecaton.{so,dylib,dll}`:
  `hc_member_start(config_ptr, len, callbacks) -> handle`, `hc_member_shutdown(handle)`,
  `hc_submit(handle, frame_ptr, len, completion)`, `hc_subscribe_events(...)`, plus
  logging and metrics sinks. `catch_unwind` at every boundary; panics become
  `HecatonException`, never a JVM abort.
- Java side (`hecaton-embedded`, Java 25): loads the library with the **FFM API**
  (`java.lang.foreign`, no JNI glue, bindings generated with `jextract`), passes the
  config as the XML produced by the existing `ConfigXmlGenerator`, and implements
  `HecatonInstance` with the **unchanged client proxies** over an in-process
  `ClientConnection`: `ClientMessage` frames go through `hc_submit` as memory segments and
  responses/events come back through FFM upcall stubs. No sockets, no copies beyond the
  frame bytes; one code path with the network client.
- Native libraries ship as classifier jars (`hecaton-native-linux-x86_64`,
  `-linux-aarch64`, `-macos-aarch64`, `-windows-x86_64`), the way RocksDB and Netty do;
  `hecaton-embedded` picks the right one at runtime.
- The member inside the JVM is a full cluster member: it joins other embedded or
  standalone Rust members, owns partitions, serves network clients on 5701.

### 7.2 User code in embedded mode

The host JVM implements `UserCodeRuntime` through upcalls: the Rust shard calls
`invokeEntryProcessor(recordView)` on a Java upcall stub, which deserializes the
`EntryProcessor` with the application's class loader and runs it against a `Map.Entry`
backed by the native record. Map stores, executors, interceptors, custom predicates and
JAAS login modules work the same way. Upcalls run on attached native threads from a
bounded pool; blocking Java code never runs on a shard thread.

### 7.3 Member-side Java API

Member-only API that applications use in embedded mode is kept and mapped:
`getCluster()` (members, state changes, `shutdown`), `getPartitionService()` (partition
owners, migration listeners, safety checks), `getLifecycleService()`, `getConfig()` with
dynamic config additions, `getCPSubsystem().getCPSubsystemManagementService()`,
`getLoggingService()`. These become protocol messages or dedicated FFI calls. Anything
that exposes Java member internals (`NodeEngine`, SPI services, `MapStore` wiring beyond
config) is removed and listed in `changes-from-hazelcast.md`.

### 7.4 Limits

- Java 25 only (FFM, `--enable-native-access`); a Rust crash in `unsafe` code can still
  take the JVM down, hence R14's `miri`/fuzzing budget.
- Jet stays unavailable in embedded mode too (RN2).
- Platform coverage is what the native jars cover; other platforms use client/server mode.

The same `hecaton-ffi` surface later gives embedded members for Python (`PyO3`) and
Node.js (`napi-rs`) at little extra cost.

---

## 8. Observability and operations

- Metrics: Prometheus/OpenMetrics endpoint with the Hecaton metric names (spec §5.2), plus
  `tracing` with OpenTelemetry export; no JMX (RN5).
- REST: same paths, same endpoint groups, `/hazelcast` alias kept; health, cluster state,
  WAN and CP management, dynamic config, `hc-cluster-admin` and `hc-cluster-cp-admin`
  reimplemented in the Rust `hc-cli` against REST and the client protocol.
- Memcache text protocol.
- Distribution: static `musl` binary, `tar.gz`/`zip` with the `hc*` scripts, container
  image (distroless), Kubernetes manifests and RBAC from `docs/kubernetes`, Helm later.
- Logging: `tracing` to stdout/JSON; log4j2-style config elements are mapped or ignored
  with an INFO line.

---

## 9. Testing strategy

### 9.1 The Java code as oracle

The existing Java sources are the executable specification:

- **Codec fixtures:** a Java generator (kept in this repo) encodes every request, response
  and event codec with fixed sample values into binary files plus a JSON description; a
  Rust test decodes and re-encodes each and requires byte equality. Runs in CI of both
  repos against the pinned protocol tag.
- **Serialization fixtures:** `Data` for every type id, Compact schemas with all field
  kinds, Portable with nested and versioned classes, partition hashes for a corpus of keys
  (strings, numbers, UUIDs, Compact and Portable keys, `PartitionAware`), JSON values.
- **Predicate/aggregator/projection fixtures:** IDS bytes for every built-in, with expected
  results against a fixed data set.
- **Error mapping:** Java exception → error code table exported from
  `ClientProtocolErrorCodes` and `ClientExceptions`.

### 9.2 Behaviour

- `compat-tests/` (official clients Java 5.5/5.6/5.7, Python, Node.js, Go, .NET) against a
  single Rust member from M1 and a three-member cluster from M2; the suite grows a scenario
  per data structure and per feature.
- Client test suite of the carved-out Java client (§6.2) against the Rust member.
- Linearizability: a Jepsen-style harness (`maelstrom`/`elle` or `turmoil` with a model
  checker) for CP structures, CPMap and IMap with sync backups under partitions and member
  kills.
- Chaos: nightly cluster tests with random member kills, network partitions
  (`toxiproxy`), clock skew, slow disks; data-loss checks up to the configured backup
  count.
- Persistence: crash-consistency suite (kill −9 during WAL append, snapshot, migration),
  30 consecutive green nightlies before 1.0 (same bar as today's M6).
- Performance: the `benchmarks/` module and `docs/results` baselines; targets p99 and p999
  at equal throughput against the Java member; memory per entry.
- Fuzzing: `cargo-fuzz` on frame decoding, `Data`/Compact/Portable decoding, predicate
  decoding, SQL parsing; corpus seeded from the fixtures.

---

## 10. Milestones

| Milestone | Content | Exit criteria | Est. |
|---|---|---|---|
| **M0** Foundations | New repo, workspace, CI (Linux x86_64/aarch64, macOS), codec generator from protocol YAML, `ClientMessage` framing, `Data` and partition hashing, Compact/Portable/JSON readers, config loader skeleton, Java oracle fixture generator, spikes: `openraft` fit, DataFusion extension points, FFM upcall latency | Fixture tests green for all 477 codecs and all serialization fixtures | 6–8 ew |
| **M1** Single member, IMap | CP2 listener, auth (simple realm), cluster/partition view events, Compact schema service, ping/statistics, error mapping, backup acks, IMap core ops with built-in predicates, entry listeners, TTL, near-cache invalidation, failover flag, Prometheus, REST health, `hc-start` | `compat-tests/` map scenarios pass for all official clients against one Rust member; Java client smoke tests pass | 10–12 ew |
| **M2** Cluster | Member protocol, join (tcp-ip, multicast, k8s), failure detection, partition table and migrations, sync/async backups, anti-entropy, graceful shutdown, cluster state, lite members, split-brain merge, member groups / `MULTI_MEMBER` routing | 3–5 member cluster passes compat tests; chaos suite shows no loss within backup count | 12–16 ew |
| **M3** Breadth | ICache, MultiMap, Queue, List, Set, Ringbuffer, Topic/Reliable, ReplicatedMap, PNCounter, FlakeId, Cardinality, transactions, indexes, aggregators/projections/paging, event journal, eviction policies, dynamic config, load protection (F23), Memcache | Per-structure compat scenarios and migrated client tests green | 14–18 ew |
| **M4** Security | TLS everywhere (F1), realms (F2) without LDAP/Kerberos, permissions (F3), REST/Memcache auth | Security suites green; `compat-tests` with TLS | 6–8 ew |
| **M5** CP Subsystem | Raft groups on `openraft`, sessions, all CP structures, CPMap, CP management REST/CLI, CP group view listener | Linearizability harness green; ported CP scenario tests | 10–12 ew |
| **M6** Persistence | WAL + snapshot per partition, metadata store, coordinated restart, partial-start policies, hot backup, CP persistence (F9), dynamic-config persistence | Crash-consistency suite green 30 nightlies | 10–12 ew |
| **M7** WAN | Batch publisher, consumer, ack types, queue-full policies, management ops, dynamic add; IMap and ICache | Multi-cluster suite green | 6–8 ew |
| **M8** Batch SQL | DataFusion engine, dialect, mappings/views/indexes DDL, DML, paging, SQL permissions | SQL compat scenarios and a ported subset of the SQL test suite green; legal sign-off on R8 | 14–20 ew |
| **M9** Embedded mode and Java carve-out | `hecaton-ffi`, native jars, `hecaton-embedded`, host-JVM user-code runtime, Java client module split, Rust member test fixture, Spring integration | Embedded Spring Boot sample and the migration samples pass against the embedded member; client test suite migrated | 12–16 ew |
| **M10** 1.0 | `hecaton-jvm` optional runtime for standalone members, audit log, LDAP, WAN sync, docs, distribution, migration guide from Java Hecaton, performance report, security review | Quality gates of spec §10; `java-member` maintenance line declared | 8–12 ew |
| **M11** Later | Vector (F20), Tiered Storage (F12), query cache, XA, Kerberos, rolling-upgrade tooling (F14), Python/Node embedded bindings, Jet decision | per feature | 30–40 ew |

**Total to 1.0 (M0–M10): 120–160 ew.** With three engineers that is roughly 12–18 months.
The critical path is M0 → M1 → M2 → M3; M4, M5, M6, M7 and M8 run in parallel after M2, and
M9 can start its Java half (client carve-out) right after M1.

```
M0 ─▶ M1 ─▶ M2 ─▶ M3 ─┬─▶ M4 security ─────────┐
                      ├─▶ M5 CP ─▶ M6 persist ─┼─▶ M10 (1.0) ─▶ M11
                      ├─▶ M7 WAN ───────────────┤
                      └─▶ M8 SQL ───────────────┘
      M1 ─▶ M9 Java client carve-out (parallel) ─▶ M9 embedded (after M3)
```

---

## 11. Risks and open questions

| Risk | Mitigation |
|---|---|
| Hidden protocol behaviour (retry semantics, event ordering, listener re-registration after reconnect, partition-lost events) that clients depend on and that is not in the codecs | Treat the Java member as the oracle: differential tests run the same client scenario against both members and diff the frame traces; the `compat-tests/` suite gets per-client reconnection scenarios early (M2) |
| `openraft` membership and snapshot model vs CP group semantics (sessions, group destruction, `cp-member-count`, priority members) | M0 spike with a go/no-go; fallback is an in-house Raft (adds ~6 ew to M5) |
| DataFusion dialect gap and plan quality for partitioned scans | Scope SQL to IMap/ReplicatedMap batch queries; publish the unsupported-syntax list; M0 spike on a custom `TableProvider` with index push-down |
| Clean-room status of Rust SQL vs HCL | Written from behaviour and the Apache-2.0 design notes only; legal review before M8; contributors who worked on `hecaton-sql` do not write the Rust SQL engine's optimizer |
| FFM upcall cost makes entry processors in embedded mode slow | Measure in M0; batch upcalls per partition for `executeOnEntries`; keep the pure in-process fast path for proxies |
| The Java member maintenance line splits the team | Time-box it (R12): security fixes only after Rust 1.0, EOL one year later |
| Unsafe code in allocators, FFI and `io_uring` | `miri`, fuzzing, `#![forbid(unsafe_code)]` everywhere except three audited crates |
| Scope creep toward Jet | RN2 is a hard rule for 1.0; revisit after M11 with a separate design |

Open questions to settle before M0 ends:

1. Does embedded mode need Java 17/21 hosts (JNI fallback) or is Java 25 acceptable?
2. Rust client crate as a public product or internal (CLI, tests) only?
3. Keep the `hc.*` property names 1:1, or take the chance to flatten the ~400 system
   properties into a documented set (with the legacy names accepted)?
4. Partition count and backup limits: keep 271/6 for migration parity, or allow larger
   counts for very large clusters (clients cope with any count)?
5. Which `MC*` codecs does `hc-cli` actually need?

---

## 12. Proposed repository layout

```
hecaton-rs/
  Cargo.toml                      # workspace, MSRV, lints, deny.toml
  crates/
    protocol/                     # ClientMessage framing, generated codecs, error codes
    serialization/                # Data, hashing, Compact, Portable, IDS built-ins, JSON
    config/                       # XML/YAML/env/CLI config model and validation
    net/                          # listeners, protocol demux, TLS, connection management
    cluster/                      # membership, discovery, failure detection, cluster state
    partition/                    # partition table, migrations, replicas, anti-entropy
    spi/                          # operations, invocation, events, proxies, shards
    client-engine/                # server side of the client protocol: tasks, listeners, schemas
    map/ cache/ collections/ topic/ replicatedmap/ crdt/ flakeid/ cardinality/ transaction/
    query/                        # predicates, indexes, aggregators, projections, paging
    cp/                           # openraft integration, CP structures, CPMap
    security/ wan/ persistence/ sql/ vector/
    user-code/                    # UserCodeRuntime trait; `jvm` feature = hecaton-jvm
    rest/ memcache/ metrics/
    ffi/                          # C ABI for embedded mode (cbindgen)
    client/                       # Rust client library
    member/                       # the `hecaton-member` binary
    cli/                          # hc-cli, hc-cluster-admin, hc-cluster-cp-admin, hc-healthcheck
  tools/protocol-gen/             # codegen from hazelcast-client-protocol YAML (pinned submodule)
  fixtures/                       # Java-oracle binary fixtures (generated in the Java repo)
  compat-tests/                   # moved from this repo
  dist/                           # scripts, Dockerfile, k8s manifests
  docs/

hecaton (this repo, after M9)
  hecaton-client/                 # the Java client, real sources
  hecaton-embedded/               # FFM shim + HecatonInstance over libhecaton
  hecaton-native-*/               # native library jars per platform (built from hecaton-rs CI)
  hecaton-usercode/               # Java module loaded by hecaton-jvm on standalone members
  hecaton-spring*/                # Spring integration
  hecaton-migrate*/               # migration tooling
  tools/fixtures/                 # Java oracle fixture generator
  java-member branch              # maintenance line of today's Java member (R12)
```
