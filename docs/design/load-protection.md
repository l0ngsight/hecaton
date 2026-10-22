# Load protection: shared payloads, bounded buffers, workload isolation, partitioned queues (F23)

|||
|---|---|
| Document status | **DRAFT — plan for review**, no code yet |
| Feature | F23 (new, Hecaton-only; not an Enterprise parity feature) |
| Milestone | M11 (proposed), branch `feature/m11-load-protection` once the plan is approved |
| Toggle | Server side, programmatic: `Config#getLoadProtectionConfig().setEnabled(true)`. Off by default |

## 1. Background

### 1.1 How we use the grid

Components running on tens of servers share a distributed map and a distributed queue under high
concurrency. The members are embedded: the application depends on the Hecaton jar and starts the
member from Java code (`Hecaton.newHecatonInstance(config)`), with a programmatic `Config` and no
XML or YAML. Everything in this plan is therefore configured through Java objects first;
declarative config support follows the usual pattern but is not needed by us.

### 1.2 Problems

1. **Memory amplification.** The map holds more than 1 million immutable entries. A popular value
   can be requested by tens of thousands of clients at once. If a 1 MB value is copied into 50,000
   outbound responses, temporary allocation approaches 50 GB: memory follows the number of
   concurrent readers instead of the stored data, GC pressure rises, and members can run out of
   memory and destabilise the cluster.
2. **Short-lived connection churn.** The cluster receives a very high number of short-lived
   connections. Disconnect cleanup, heartbeats and client events share threads and queues with map
   and queue operations, so maintenance work delays useful work.
3. **Queue hotspotting.** One queue lives on one partition, owned by one member. The application
   spreads work over many physical queues by hashing on the client. That works, but duplicates
   routing logic in every client and hides the logical queue from the server.

### 1.3 Principles

Store once and share safely. Bound every queue and buffer. Throttle before memory runs out.
Separate map, queue, connection and maintenance workloads. Keep partitions and owners transparent
to clients. Make failure behaviour measurable and testable.

### 1.4 Goals and non-goals

Goals, in priority order (the first is the mandatory first deliverable):

1. Remove, or strictly bound, per-request duplication of map values (§4.2, §4.3).
2. Bound all per-connection and member-wide buffers, with back pressure before exhaustion (§4.4).
3. Isolate connection-lifecycle work from data operations (§4.5).
4. Observability for all of the above (§4.7).
5. One logical queue partitioned across members (§4.6) — nice to have, after 1–4.

Non-goals:

- Changing the client protocol or requiring a new client. Official Hazelcast/Hecaton clients must
  work unchanged (a new optional client helper is allowed, §4.6.4).
- Zero-copy for `OBJECT` in-memory format, mutable maps, SQL, Jet, or the TPC (`alto`) path.
- Changing the behaviour of a cluster that does not enable the feature.

### 1.5 Terminology

| Term | Meaning |
|---|---|
| Immutable map | A map whose entries cannot be changed after a successful insert (§4.2) |
| Shared payload | One read-only, reference-counted buffer holding a value's wire bytes, referenced by every response that sends it (§4.3) |
| Logical bytes | Bytes queued for sending, counted per response (50,000 × 1 MB = 50 GB) |
| Physical bytes | Distinct bytes actually held by outbound queues (the same 1 MB counted once) |
| Maintenance lane | Threads and queues reserved for connection, endpoint and heartbeat work (§4.5) |
| Shard | One physical queue that is part of a logical partitioned queue (§4.6) |

## 2. Requirements and where they are met

| # | Requirement | Priority | Where |
|---|---|---|---|
| FR1 | Horizontally scalable distributed key-value map | mandatory | existing `IMap`; §4.2 adds nothing that limits scaling |
| FR2 | At least 1 million entries within configured capacity | mandatory | existing; Phase 0 benchmark sizes the members (§6) |
| FR3 | Keys and values immutable after a successful insert | mandatory | §4.2 |
| FR4 | No full payload copy per concurrent reader | mandatory | §4.3 |
| FR5 | Peak design target: 50,000 concurrent client requests or connections | mandatory | §4.4, §4.5, perf test §6 |
| FR6 | Configurable replication and automatic recovery from member failure | mandatory | existing backups and migration; §4.2.4 |
| FR7 | One logical distributed queue exposed to clients | nice to have | §4.6 |
| FR8 | Queue traffic partitioned across several members | nice to have | §4.6 |
| FR9 | Transparent partition routing and failover for the queue | nice to have | §4.6 |
| FR10 | Client-side sharding still supported during migration | nice to have | §4.6.4 |
| NFR Memory | Read concurrency must not cause value size × reader count growth | mandatory | §4.3, §4.4 |
| NFR Scalability | Capacity and throughput grow with members within measured limits | mandatory | §4.2.4, §6 |
| NFR Availability | No single member is a single point of failure for map data or acknowledged queue items | mandatory | existing backups; §4.6.5 |
| NFR Back pressure | Reject, defer or throttle before memory exhaustion | mandatory | §4.4 |
| NFR Isolation | Connection cleanup must not starve map or queue operations | mandatory | §4.5 |
| NFR Observability | Metrics for connections, queued bytes, references, partition load, replication lag, rejections, cleanup backlog | mandatory | §4.7 |

## 3. What the code does today

Read from the code on `main` (817b97a8). Phase 0 (§5) confirms each point with a measurement
before we change anything.

### 3.1 Where a map value is copied on its way to a client

The path of a client `IMap.get`: `ClientEngineImpl#accept` → `MapGetMessageTask` (partition
thread) → `GetOperation#runInternal` → `RecordStore#get` → `MapGetMessageTask#encodeResponse` →
`MapGetCodec.encodeResponse` → `DataCodec.encode`, which adds
`new ClientMessage.Frame(data.toByteArray())` → the connection's `NioOutboundPipeline.writeQueue`
→ `ClientMessageEncoder` / `ClientMessageWriter` copy into the connection's send buffer.

| Case | Per-request copy of the value? | Where |
|---|---|---|
| `BINARY` (default), client connected to the partition owner | **No copy.** `HeapData#toByteArray` returns the record's own array, and the frame references it | `HeapData#toByteArray`, `DataCodec#encode` |
| `BINARY`, value replaced while responses are queued | No copy, but **retention**: every queued response pins the version it read. A large value that is updated while 50,000 slow responses are queued keeps every pinned version alive | `NioOutboundPipeline.writeQueue` (unbounded) |
| `OBJECT` | **Full copy per request**: `serializationService.toData(response)` serialises the value again for every get | `MapGetMessageTask#encodeResponse` |
| `NATIVE` (HD) | **Full copy per request**: each read copies the native block into a new heap array | `NativeData#read` (`hecaton-hd`) |
| Tiered Storage | **Full copy per request** from memory or disk tier | `hecaton-tiered` |
| Client connected to a member that does not own the partition (single-member/unisocket routing, stale partition table, migration) | **Two copies per request**: the owner serialises the response into a `Packet`, the receiving member allocates a new array for it | operation response path |
| TPC (`alto`) enabled | Copy into an `IOBuffer` per response | `AbstractMessageTask` (TPC is not in scope, §1.4) |
| Socket write, TLS | One copy into the connection's send buffer / TLS buffer, **bounded** by the buffer size | `ClientMessageWriter`, TLS handler |

So the 50 GB scenario is real for `OBJECT`, `NATIVE`, tiered maps and for non-owner members, and
for `BINARY` maps it appears as retention through unbounded outbound queues rather than as copies.
Both are addressed: §4.3 removes the copies, §4.4 bounds the retention.

### 3.2 Unbounded buffers and missing admission control

- `NioOutboundPipeline.writeQueue` and `priorityWriteQueue` are unbounded `ConcurrentLinkedQueue`s.
  Pending bytes are already counted (`writeQueuePendingBytes`) but never limited, so a slow or
  stalled client accumulates responses without bound.
- `ClusterProperty.CLIENT_PROTOCOL_UNVERIFIED_MESSAGE_BYTES` limits only messages from
  unauthenticated connections; authenticated clients can send messages of any size.
- No member-side limit on in-flight client requests per connection or per member. Partition
  operation queues are unbounded. The `BACKPRESSURE_*` properties apply to member-to-member
  invocations only.

### 3.3 Connection lifecycle work shares threads with data work

- `TcpServerConnectionManagerBase#fireConnectionRemovedEvent` runs `ClientEngineImpl`'s
  `connectionRemoved` on the **event service's striped executor** — the same threads that deliver
  entry, item and topic events to clients.
- Each disconnect logs `Destroying ...` at INFO (`ClientEndpointManagerImpl#removeEndpoint`).
- `ClientEndpointImpl#destroy` → `OperationParkerImpl#onClientDisconnected` walks **every wait set**
  for **each** disconnected client: O(clients × wait sets) under churn.
- `ClientLifecycleMonitor` runs `getClientsInCluster()`, a blocking invocation on every member, on
  the shared scheduled executor.
- Authentication of new connections runs on the generic/urgent operation threads.

### 3.4 Queue placement

`QueueService` places a queue on the partition of its name (`StringPartitioningStrategy`), so all
offers and polls of a queue run on one partition thread of one member.

## 4. Design

### 4.1 Configuration and the toggle

One server-side switch turns the whole feature on or off. It is read at member start; with it off,
none of the new code paths are created and the member behaves exactly as today (the inherited test
suite runs with the default, off).

```java
Config config = new Config();

LoadProtectionConfig lp = config.getLoadProtectionConfig()
        .setEnabled(true);                                   // the toggle; default false

lp.getOutboundConfig()
        .setMaxPendingBytesPerConnection(64L << 20)          // logical bytes queued per connection
        .setMaxPhysicalPendingBytes(512L << 20)              // distinct bytes held by all outbound queues
        .setSlowConsumerTimeoutSeconds(30);                  // close a connection that does not drain

lp.getAdmissionConfig()
        .setMaxInFlightRequestsPerConnection(1_000)
        .setMaxInFlightRequests(100_000)                     // member-wide
        .setMaxMessageBytes(16 << 20)                        // authenticated clients too
        .setHeapRejectThreshold(0.85f);                      // old-gen occupancy after GC

lp.getMaintenanceConfig()
        .setThreadCount(2)
        .setMaxBacklog(200_000)
        .setMaxAcceptsPerSecond(5_000)
        .setMaxConcurrentAuthentications(256);

config.getMapConfig("catalog")
        .setInMemoryFormat(InMemoryFormat.BINARY)            // or NATIVE
        .setImmutable(true);                                 // §4.2

config.getQueueConfig("jobs")
        .setShardCount(8);                                   // §4.6, nice to have

HecatonInstance member = Hecaton.newHecatonInstance(config);
```

- The same switch is available as the property `hecaton.load-protection.enabled`
  (`config.setProperty(...)` or `-D`), so operators can turn it off without a code change.
- With the toggle off, `MapConfig#setImmutable(true)` and `QueueConfig#setShardCount(n > 1)` are
  ignored with one WARNING at start that names the maps and queues. With it on, they take effect.
- Defaults for the limits are placeholders until Phase 0 measures real numbers (❓ §7).
- `MapConfig#isImmutable` and `QueueConfig#getShardCount` take part in the existing config-equality
  checks (static and dynamic config), so every member applies the same rules to the same structure.
- Declarative (YAML/XML) and Spring support: added for completeness in the last phase; not needed
  for embedded, programmatic use.
- Name ❓: `LoadProtectionConfig` is a working name.

### 4.2 Immutable maps (FR3)

#### 4.2.1 Semantics

A map with `setImmutable(true)` accepts an insert of an absent key and refuses any operation that
would change the value of a present key.

| Operation on a present key | Result |
|---|---|
| `putIfAbsent`, `putIfAbsentAsync` | returns the existing value (unchanged behaviour) |
| `set`, `put`, `putAll`, `replace`, `merge`, entry processors that call `setValue`, `MapLoader` reload | refused with `ImmutableEntryException` (extends `IllegalStateException`, so official clients surface it as `IllegalStateException` with the message) |
| `get`, `getAll`, queries, read-only entry processors | unchanged |
| `remove`, `delete`, eviction, expiry | allowed (❓ §7: forbid instead?) |
| insert after remove | allowed; it is a new entry |

Checks run on the partition owner inside the operation, so they are linearisable with inserts.
Keys are immutable already (a key is the identity of an entry). Split-brain merge policies run with
"existing wins" semantics for immutable maps.

#### 4.2.2 Restrictions validated at start

`ConfigValidator` refuses an immutable map with `OBJECT` format (nothing to share), with a
`MapStore` in write-behind mode (would rewrite values).

#### 4.2.3 What immutability buys

- Shared payloads need no copy-on-write and no versioning: a payload is valid until the entry is
  removed (§4.3).
- Reads of the same key can be coalesced and cached on non-owner members without invalidation
  storms (§4.3.4).
- Backups hold identical bytes; read-from-backup becomes safe for immutable maps (§4.2.4).

#### 4.2.4 Replication and recovery (FR6, NFR availability, NFR scalability)

Unchanged mechanisms: `backup-count`/`async-backup-count`, partition migration, anti-entropy. One
addition: for immutable maps, a client `get` that arrives at a member holding a backup replica of
the key's partition is answered from that replica instead of being forwarded to the primary. Today
`readBackupData` applies only to member-side proxies (`MapProxySupport`); for immutable maps the
client message task does the same, which spreads a hot key's reads over `1 + backup-count` members
for clients that are not routed to the primary.

### 4.3 Shared, transmission-ready payloads (FR4, NFR memory)

#### 4.3.1 Representation

`SharedPayload` (core, `--release 17`): a read-only `ByteBuffer` holding the value's serialised
bytes exactly as they appear in a client message frame, its length, and a reference count. It
implements `Data`, so existing code that handles `Data` keeps working.

| Storage | How the shared payload is made | Cost |
|---|---|---|
| `BINARY` | Wraps the record's existing `HeapData` array (`ByteBuffer.wrap(...).asReadOnlyBuffer()`); created once per record, lazily, on the first read under load protection | one small object per hot record |
| `NATIVE` | `hecaton-hd` exposes the native block as a read-only `ByteBuffer` view (`MemorySegment#asByteBuffer`) | no heap copy |
| Tiered | the memory tier as for `NATIVE`; a disk read creates one payload that concurrent readers share (§4.3.4) | one copy per disk read, not per reader |

#### 4.3.2 Sending without copying

- `ClientMessage.Frame` gets a second kind of content: a reference to a `SharedPayload` instead
  of a `byte[]`. The bytes on the wire are identical, so clients see no difference.
- `DataCodec#encode` adds a shared frame when the `Data` is a `SharedPayload`. `MapGetCodec`,
  `MapGetAllCodec` and the codecs that return entry sets/values in pages use `DataCodec`, so they
  are covered without codec changes.
- `ClientMessageWriter` writes a shared frame by putting a `duplicate()` of the buffer into the
  connection's send buffer chunk by chunk, keeping its position in the frame like it does for
  arrays today. The only copy left is the bounded, per-connection send/TLS buffer (§3.1, last row).
- References: a reference is taken when a response is queued and released when its last byte is
  written, or when the connection closes and its outbound queue is drained (the close path must
  release every queued frame — covered by a dedicated test).

#### 4.3.3 Lifetime of native payloads

Removing, evicting, expiring or migrating an entry of a `NATIVE` immutable map **retires** its
block instead of freeing it: the block is freed when the reference count reaches zero. The retired
blocks are counted against native memory and reported (`retiredBytes`), and the outbound limits
(§4.4) bound how long a retired block can stay pinned (a stalled connection is closed after
`slowConsumerTimeoutSeconds`).

#### 4.3.4 Requests that do not reach the owner directly

- **Read coalescing (single flight).** On the member that serves a client, concurrent `get`s of the
  same key of an immutable map that need a remote invocation share one in-flight invocation; every
  waiting request is answered with the same payload. Applies equally to tiered disk reads on the
  owner.
- **Hot value cache (optional, Phase 3).** A bounded (bytes), LRU cache of shared payloads of
  immutable maps on non-owner members, invalidated by the existing near cache invalidation events
  on remove. Off unless sized.

#### 4.3.5 What remains per request

One `ClientMessage`, a few small frames and one reference count increment — independent of the
value size. The Phase 1 exit criterion is measured as **allocated bytes per hot `get` must not grow
with the value size** (§6).

### 4.4 Bounded buffers and back pressure (NFR back pressure, NFR memory)

Accounting:

- Per connection: logical pending bytes and messages (exists, made observable and limited).
- Per member: logical pending bytes; **physical** pending bytes — a `SharedPayload` adds its size
  when its outbound reference count goes 0 → 1 and subtracts it on 1 → 0, so 50,000 queued
  responses of one 1 MB value count 1 MB; in-flight client requests (decoded, not yet answered).

Actions, cheapest first:

1. **Defer.** When a connection is over `maxPendingBytesPerConnection` or
   `maxInFlightRequestsPerConnection`, the member stops reading from that connection (removes read
   interest from its inbound pipeline) until it drains below a low watermark. TCP flow control then
   pushes back on the client. Works with every client, no protocol change.
2. **Throttle.** When the member is over `maxInFlightRequests` or `maxPhysicalPendingBytes`, it
   stops reading from all client connections in round robin, keeping member-to-member connections
   and the maintenance lane running.
3. **Reject.** A request that is decoded while the member is above the hard watermark, or while heap
   occupancy after the last GC exceeds `heapRejectThreshold`, is answered with `HECATON_OVERLOAD`
   (`HecatonOverloadException`, error code 20, known to official clients). Requests whose response
   is known to be large (reads of keys whose payload is above a size threshold) are rejected first.
4. **Disconnect.** A connection whose outbound queue has not drained for
   `slowConsumerTimeoutSeconds` is closed; its queued frames are released.

Also bounded: authenticated client messages (`maxMessageBytes`, checked while reading the frame
header, before allocating), and client-originated operations per partition thread (the
`BackpressureRegulator` idea applied to client requests).

### 4.5 Workload isolation (NFR isolation, problem 2)

1. **Maintenance lane.** A dedicated executor `hz:client-maintenance` (fixed small thread count,
   bounded queue `maxBacklog`) runs: client `connectionRemoved` handling, endpoint destroy,
   `ClientAwareService` cleanup, transaction rollback for departed clients,
   `ClientLifecycleMonitor` and `ClientHeartbeatMonitor`. The event service's threads go back to
   delivering events only.
2. **Batched disconnects.** Disconnected client UUIDs are collected and processed in batches (every
   100 ms or 1,000 clients). `OperationParker` walks the wait sets **once per batch** with a set of
   UUIDs, as a time-sliced task per partition thread (at most ~1 ms per slice, then re-queued), so a
   disconnect storm cannot occupy partition threads.
3. **Connection admission.** Accept rate (`maxAcceptsPerSecond`) and concurrent authentications
   (`maxConcurrentAuthentications`) are limited; authentications above the limit wait in a bounded
   queue and beyond it the connection is closed before authentication. Authentication moves off the
   generic operation threads to the maintenance lane's auth pool.
4. **Quiet churn.** `Destroying ...` goes to FINE; a periodic summary line reports opened/closed
   connections and cleanup backlog.
5. **Backlog overflow.** If the maintenance backlog is full, new connections are refused (step 3)
   rather than delaying data operations. Cleanup work is never dropped: endpoints are removed from
   the connection map immediately; only the expensive cleanup is deferred.

### 4.6 Partitioned logical queue (FR7–FR10, nice to have)

#### 4.6.1 Model

`QueueConfig#setShardCount(n)` with `n > 1` makes `jobs` a logical queue of `n` shards, each an
ordinary queue with its own partition, backups and migration. Shards are placed on partitions
spread evenly over the partition range (partition of `jobs` + i × partitionCount / n), which the
partition table spreads over members; Phase 4 measures whether that is even enough or a placement
map is needed (❓ §7).

#### 4.6.2 Transparent routing (FR9)

Official clients keep sending `QueueOffer`, `QueuePoll`, ... to the partition of `jobs`. The
member's message task sees a sharded queue and routes on the member:

- `offer`: to the shard owned by the local member if there is one with room, else round robin over
  shards weighted by their sizes (sizes are piggybacked on responses and refreshed periodically).
- `poll`/`take`: the local shard first, then the others; a blocking `poll`/`take` registers one
  member-local waiter that is woken by an offer to any shard, with the caller's timeout.
- `size`, `isEmpty`, `drainTo`, `iterator`, `contains`: fan out to all shards and combine.
- Item listeners: registered on every shard.

The extra hop (client → routing member → shard owner) is the price of transparency. Phase 4 adds an
optional smart path for Hecaton's Java client that offers directly to a shard.

#### 4.6.3 Ordering

FIFO holds per shard, not across the logical queue. Documented; queues that need global FIFO keep
`shardCount = 1` (❓ §7).

#### 4.6.4 Migrating from client-side hashing (FR10)

`QueueConfig#setShardNames("jobs-0", ..., "jobs-7")` declares existing physical queues as the shards
of the logical queue, without moving data. Old clients that hash themselves keep using `jobs-i`
directly; new clients use `jobs`. Both see the same items. Once all clients are moved, the client
hashing code can be removed.

#### 4.6.5 Failure behaviour

Each shard is a normal queue: acknowledged items are on `1 + backup-count` members; when a member
fails, its shards' backups are promoted. A waiting `take` on the routing member survives the loss
of a shard owner; if the routing member itself fails, the client retries as it does today.

### 4.7 Observability

Metrics in the existing registry (JMX, Prometheus exporter), prefix `loadProtection`:

| Area | Metrics |
|---|---|
| Connections | active, opened/s, closed/s, refused (by reason), authentications in progress / queued |
| Queued bytes | outbound logical bytes and messages (member, top-N connections), outbound physical bytes, reads suspended (connections) |
| References | shared payloads alive, references held, retired native bytes |
| Partition load | operations and queue depth per partition thread, hot keys (top-N by reads, sampled) |
| Replication lag | pending backup operations, async backup lag, anti-entropy syncs |
| Rejections | by reason: overload, heap, message too large, slow consumer disconnect, immutable violation |
| Cleanup backlog | maintenance queue length, age of oldest task, disconnect batches, wait-set scan time |
| Coalescing | coalesced reads, hot value cache hits/misses/bytes |
| Queues | per shard size and offer/poll rate, routing hops |

### 4.8 Compatibility and rollout

- Wire protocol unchanged; official clients need no change.
- Off by default; with it off nothing changes (verified by the inherited suite).
- Mixed-version clusters: enabling the feature requires every member on the version that has it
  (cluster version gate, F14); `isImmutable`/`shardCount` are in config equality.
- `docs/changes-from-hazelcast.md` gets the new config and exception; `docs/load-protection.md`
  is the user guide.

### 4.9 Alternatives considered

- **Client-side caching only (near cache).** Reduces reads but does not bound the server when 50,000
  clients start cold at the same moment; still useful alongside.
- **Copying into pooled direct buffers per request.** Bounded allocation but still proportional to
  readers in bandwidth and pool size; shared payloads avoid the copy entirely.
- **Making every map zero-copy.** Mutable values need copy-on-write and version pinning; immutability
  gives the safety for free and is what our data needs.
- **A new queue type instead of sharding `IQueue`.** Would need new codecs and client support;
  sharding keeps the `IQueue` API and the official clients.

## 5. Phases

| Phase | Content | Exit criteria | Est. |
|---|---|---|---|
| **0 Measure** | Benchmark and allocation harness (`benchmarks/`, `tools/perf`): 3–5 members, 1M entries, a hot 1 MB value, 5k concurrent readers in CI and 50k on the perf rig, slow readers, connection churn (open-auth-get-close loops). Records allocation per request (JFR / `ThreadMXBean#getThreadAllocatedBytes`), heap after GC, p99 of unrelated map/queue ops. Confirms or corrects §3 | Report in `docs/performance.md`; every row of §3.1 confirmed by a number | 1–2 ew |
| **1 Shared payloads + bounds** (mandatory first) | `LoadProtectionConfig` + toggle, immutable maps, `SharedPayload` and shared frames for `BINARY` and `NATIVE`, outbound accounting and limits, admission (defer/throttle/reject/disconnect), message size limit, metrics for these | Allocation per hot `get` does not grow with value size; 50k × 1 MB reads keep heap growth below a fixed bound; no OOM with stalled readers; toggle off = inherited suite unchanged | 5–7 ew |
| **2 Isolation** (mandatory) | Maintenance lane, batched disconnects, accept/auth limits, quiet churn, cleanup metrics | Under a churn of N connections/s (N from Phase 0), p99 of map/queue ops within X% of the no-churn baseline; cleanup backlog bounded and drained | 3–4 ew |
| **3 Remote reads** | Read coalescing, read-from-backup for immutable maps, tiered maps, optional hot value cache | Non-owner and tiered reads: one copy per distinct read, not per reader | 2–3 ew |
| **4 Partitioned queue** (nice to have) | `shardCount`, member-side routing, blocking take across shards, `shardNames` migration, optional smart client path | Throughput of one logical queue scales with members up to measured limit; no acknowledged item lost when a member is killed | 4–6 ew |
| **5 Hardening and docs** | Declarative/Spring config, user guide, chaos/soak tests, defaults from measurements | Soak 24 h at design peak with no heap growth trend | 2 ew |

Phases 1 and 2 are independent after Phase 0 and can run in parallel. Each phase is one PR into
`feature/m11-load-protection`, following CONTRIBUTING (tests, multi-member and failure tests,
DCO, fork log entry).

## 6. Testing

- **Unit:** `SharedPayload` reference counting (including close-while-queued and retire-then-free),
  shared-frame writing across partial writes and TLS, immutable-map operation matrix (§4.2.1) for
  every map operation and entry processor kind, limits and watermarks, disconnect batching.
- **Allocation tests (CI):** allocated bytes per hot `get` measured for 1 KB, 100 KB and 1 MB
  values; the test fails if it grows with the size. Same for `NATIVE`.
- **Multi-member:** reads during migration and member loss of an immutable map; coalesced reads when
  the owner dies mid-invocation; partitioned queue offers/takes while a shard owner is killed — no
  acknowledged item lost, none duplicated beyond the existing at-least-once rules.
- **Back pressure:** stalled clients (stop reading the socket) — member heap stays bounded, the
  slow consumer is disconnected, other clients unaffected; overload rejections counted.
- **Isolation:** churn generator alongside a steady map/queue workload; asserts p99 bound and
  bounded maintenance backlog.
- **Toggle:** feature off — inherited suites unchanged (CI as today); feature on — the map and
  queue suites run again with load protection enabled (nightly).
- **Performance/soak:** Phase 0 rig at 50k concurrent clients; 24 h soak in Phase 5.
- **Compatibility:** official Java client (and one non-Java client from `compat-tests/`) against a
  member with the feature on; the wire is unchanged.

## 7. Open questions

- ❓ **Remove and re-insert.** Should immutable maps allow `remove`/eviction/expiry (planned: yes),
  or be strictly append-only?
- ❓ **"Mutable" value in the problem statement.** The background mentions the same large *mutable*
  value; the requirements say values are immutable after insert. Plan assumes updates become new
  keys (or remove + insert). If in-place updates of hot values must stay, Phase 1 needs version
  pinning for mutable maps too (bounded by §4.4, but more work).
- ❓ **In-memory format in production.** `BINARY`, `OBJECT` or `NATIVE`? It decides whether the
  observed growth is copying or retention (§3.1) and the order of work inside Phase 1.
- ❓ **Client types and routing.** Official Java client with smart (all-members) routing, or
  single-member/unisocket? With single-member routing, §4.3.4 moves into Phase 1.
- ❓ **TLS.** Enabled between clients and members? It adds one bounded per-connection copy that
  cannot be shared (§3.1).
- ❓ **Global FIFO.** Does any queue need global ordering across shards?
- ❓ **Default limits.** Placeholders in §4.1 until Phase 0 numbers are in.
- ❓ **Names.** `LoadProtectionConfig`, `MapConfig#setImmutable`, `QueueConfig#setShardCount`.
