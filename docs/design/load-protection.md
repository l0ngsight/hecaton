# Load protection: shared payloads, bounded buffers, workload isolation, partitioned queues (F23)

|||
|---|---|
| Document status | **Implemented** (phases 1, 2, 4 and the declarative config of 5), see §8 for what differs from this plan. Revision 3 of the plan: second review's answers applied (§1.6) and Phase 0 measurements in (§3.1) |
| Feature | F23 (new, Hecaton-only; not an Enterprise parity feature) |
| Milestone | M11, branch `claude/stoic-tesla-ny96s7` (one PR for the plan and the implementation) |
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

1. Remove, or strictly bound, per-request duplication and retention of map values (§4.2, §4.3).
2. Bound all per-connection and member-wide buffers, with back pressure before exhaustion (§4.4).
3. Isolate connection-lifecycle work from data operations (§4.5).
4. Observability for all of the above (§4.7).
5. One logical queue partitioned across members (§4.6) — nice to have, after 1–4.

Non-goals:

- Changing the client protocol or requiring a new client. Official Hazelcast/Hecaton clients must
  work unchanged (a new optional client helper is allowed, §4.6.5).
- Zero-copy for `OBJECT`, `NATIVE` or tiered maps in the first phases (we run `BINARY`, §1.6);
  they are an optional later phase (§5, phase 3).
- Mutable maps, SQL, Jet, or the TPC (`alto`) path.
- Changing the behaviour of a cluster that does not enable the feature.

### 1.5 Terminology

| Term | Meaning |
|---|---|
| Immutable map | An insert-only map: an entry cannot be changed, removed or inserted again once inserted (§4.2) |
| Shared payload | One read-only, reference-counted view of a stored value's bytes, referenced by every response that sends it (§4.3) |
| Logical bytes | Bytes queued for sending, counted per response (50,000 × 1 MB = 50 GB) |
| Physical bytes | Distinct bytes actually held by outbound queues (the same 1 MB counted once) |
| Connection buffers | The socket-side buffers a connection holds: inbound and outbound protocol buffers, TLS buffers (§3.2) |
| Maintenance lane | Threads and queues reserved for connection, endpoint and heartbeat work (§4.5) |
| Shard | One physical queue that is part of a logical partitioned queue (§4.6) |

### 1.6 Decisions from the reviews

| Question | Answer | Effect on the plan |
|---|---|---|
| Remove and re-insert on immutable maps? | **No. Reject duplicate inserts** | Immutable maps are insert-only: no remove, delete, clear, eviction or expiry (§4.2.1). No tombstones and no payload retirement are needed |
| "Mutable" value in the problem statement? | Typo: values are large and **immutable** | No version pinning; one stored version per key |
| In-memory format in production? | **`BINARY`** | Phase 1 targets `BINARY` only; `NATIVE`/tiered sharing becomes optional (phase 3). On `BINARY` the stored array is already shared with responses, so the growth must come from elsewhere (§3.1, §3.2) |
| Client routing? | **All members** (smart routing) | Client gets go straight to the partition owner (measured: 0 copies, §3.1). Reads between members still matter for the services' embedded members (measured: 4–5 copies) and are in Phase 1 (§4.3.3). Read-from-backup is dropped: smart clients never send a get to a backup |
| TLS between clients and members? | **Yes** | Per-connection TLS buffers become a first-class memory item (§3.2, §4.4.2); TLS encryption cost of hot reads lands on the owner's I/O threads (§4.4.4) |
| Global FIFO on partitioned queues? | **Preferred, best effort is fine** | Polls prefer the shard with the oldest head (§4.6.4) |
| Production version, heap dumps? | **Hazelcast 5.7**; no heap dump or histogram (the members run with a 200 GB heap); our analysis suggests the value is copied several times | Phase 0 measures it with Flight Recorder on Hazelcast CE 5.7.0 itself and on Hecaton (§3.1) |
| Stop-gap: smaller client socket buffers? | Will try | — |
| Client-side changes? | **Server side only for now** | Hot-key spreading through clients (§4.4.4) is out; everything in §4 stays member side |
| Which path do the hot reads take? | **The services read through the `IMap` of the member they embed** | Production is on the measured 4-copy path (§3.1). Reads between members (§4.3.3) are the first item of Phase 1 |

## 2. Requirements and where they are met

| # | Requirement | Priority | Where |
|---|---|---|---|
| FR1 | Horizontally scalable distributed key-value map | mandatory | existing `IMap`; §4.2 adds nothing that limits scaling |
| FR2 | At least 1 million entries within configured capacity | mandatory | existing; Phase 0 sizes the members (§5) |
| FR3 | Keys and values immutable after a successful insert | mandatory | §4.2 |
| FR4 | No full payload copy per concurrent reader | mandatory | §4.3 |
| FR5 | Peak design target: 50,000 concurrent client requests or connections | mandatory | §4.4 (connection buffers, admission), §4.5, perf test §6 |
| FR6 | Configurable replication and automatic recovery from member failure | mandatory | existing backups and migration; §4.2.3 |
| FR7 | One logical distributed queue exposed to clients | nice to have | §4.6 |
| FR8 | Queue traffic partitioned across several members | nice to have | §4.6 |
| FR9 | Transparent partition routing and failover for the queue | nice to have | §4.6 |
| FR10 | Client-side sharding still supported during migration | nice to have | §4.6.5 |
| NFR Memory | Read concurrency must not cause value size × reader count growth | mandatory | §4.3, §4.4 |
| NFR Scalability | Capacity and throughput grow with members within measured limits | mandatory | §4.4.4, §6 |
| NFR Availability | No single member is a single point of failure for map data or acknowledged queue items | mandatory | existing backups; §4.2.3, §4.6.6 |
| NFR Back pressure | Reject, defer or throttle before memory exhaustion | mandatory | §4.4 |
| NFR Isolation | Connection cleanup must not starve map or queue operations | mandatory | §4.5 |
| NFR Observability | Metrics for connections, queued bytes, references, partition load, replication lag, rejections, cleanup backlog | mandatory | §4.7 |

## 3. What the code does today

Read from the code on `main` (817b97a8), and for §3.1 measured (Phase 0).

### 3.1 Where a map value is copied: measured

The path of a client `IMap.get`: `ClientEngineImpl#accept` → `MapGetMessageTask` (partition
thread) → `GetOperation#runInternal` → `RecordStore#get` → `MapGetMessageTask#encodeResponse` →
`MapGetCodec.encodeResponse` → `DataCodec.encode`, which adds
`new ClientMessage.Frame(data.toByteArray())` → the connection's `NioOutboundPipeline.writeQueue`
→ `ClientMessageEncoder` / `ClientMessageWriter` copy into the connection's send buffer →
`TlsEncoder` encrypts into its own buffer → socket.

`benchmarks/…/fanout/HotReadAllocation` with `tools/perf/hot-read-allocation.sh` runs a two-member
cluster and its readers in one JVM, stores one 1 MB value, reads it 2,000 times with 256 reads in
flight, and records the reads with Flight Recorder. With fixed 64 KB TLABs every allocation above
64 KB is recorded with its stack, so the count of value-sized objects is exact; total bytes per
thread come from `ThreadMXBean`. Each scenario ran on **Hazelcast CE 5.7.0** and on Hecaton; the
member-side numbers are the same on both (Hecaton keeps 5.7's code on these paths), so the table
applies to production. Results and how to run it: [performance.md](../performance.md#hot-reads-of-a-large-value).

| Read path | Value-sized objects allocated in the members per read | Where |
|---|---|---|
| Client connected to the key's owner (smart routing), `BINARY` | **0** (2 KB of other objects) | the response frame references the record's array |
| — the same with TLS (Hecaton) | **0**, plus ~80 KB of TLS cipher objects per 1 MB sent | `SSLEngine#wrap`: JDK AES-GCM objects per TLS record |
| Client connected to a member that does not own the key | **3** | owner: response packet (`OutboundResponseHandler#toNormalResponsePacket`); receiver: packet read (`PacketIOHelper#readValue`), response deserialised (`NormalResponse#readData`) |
| `OBJECT` in-memory format | **2** | serialisation per read: output buffer growth and `toByteArray` |
| **Embedded member reading a key owned by another member**, blocking `get` | **4** | as the row above, plus the conversion to the caller's `byte[]` (`MapProxySupport#toObject`) |
| — the same with `getAsync` and a callback | **5** | `InvocationFuture#resolve` deserialises the response packet **once per stage waiting on the future** |
| — the same with a near cache on the members, `BINARY` | 1 | the conversion to the caller's object on each read |
| — the same with a near cache on the members, `OBJECT` | **0** (14× the throughput) | the cached object is shared by all readers |
| — the same, near cache cold, 256 concurrent first reads | 0.75 (48 of the 256 reads went to the owner, 4 copies each) | concurrent misses are not coalesced |
| A new client per read (short-lived connections) | 0, but **0.66 MB of member allocation per connection** (1.7 MB with TLS) | connection buffers and handshake (§3.2) |

Copies are not only allocation: each one stays reachable until its response is written to the
reader. 50,000 reads in flight of a 1 MB value on a 4-copy path hold about 200 GB, which would
explain the heap our members need. **Our services read through the `IMap` of the member they embed
(§1.6), so production is on that path**: every read of a key that another member owns costs 4
copies (blocking `get`) or 5 (`getAsync`). The `BINARY` + smart-client path, by contrast, does not
copy at all. A near cache on the members removes the copies once it holds the value, but not for
the first burst of a key: concurrent misses each go to the owner.

Not measured (we do not run them): `NATIVE` copies per read into a new heap array
(`NativeData#read`), tiered storage likewise, and TPC (`alto`) into an `IOBuffer` per response.

### 3.2 Per-connection and per-request memory

- **Connection buffers are allocated eagerly and are large.** For a TLS client connection, every
  connection allocates when its pipeline is built, whether or not it ever sends anything:
  `TlsDecoder` source buffer (≥ `SO_RCVBUF`, 128 KB by default), its plaintext buffer (TLS
  application buffer size, ~16 KB), `ClientMessageDecoder` source buffer (128 KB),
  `ClientMessageEncoder` destination buffer (128 KB) and `TlsEncoder` destination buffer
  (≥ `SO_SNDBUF`, 128 KB). That is **about 530 KB per connection**, on the heap
  (`hecaton.socket.buffer.direct=false`). **50,000 connections need about 26 GB before any data
  moves.** Defaults: `ClusterProperty.SOCKET_RECEIVE_BUFFER_SIZE` / `SOCKET_SEND_BUFFER_SIZE`
  (128 KB); `SOCKET_CLIENT_*_BUFFER_SIZE` (`-1`, i.e. the same).
- **Outbound queues are unbounded.** `NioOutboundPipeline.writeQueue` and `priorityWriteQueue` are
  `ConcurrentLinkedQueue`s. Pending bytes are counted (`writeQueuePendingBytes`) but never limited.
  On `BINARY` the queued responses share the stored array, so the queue holds message objects and
  frames, not value copies; but there is no bound on how many.
- **Inbound size.** `CLIENT_PROTOCOL_UNVERIFIED_MESSAGE_BYTES` limits only unauthenticated
  connections; authenticated clients can send messages of any size.
- **No admission control for client requests.** No limit on in-flight client requests per
  connection or per member; partition operation queues are unbounded. The `BACKPRESSURE_*`
  properties apply to member-to-member invocations only.

### 3.3 Connection lifecycle work shares threads with data work

- `TcpServerConnectionManagerBase#fireConnectionRemovedEvent` runs `ClientEngineImpl`'s
  `connectionRemoved` on the **event service's striped executor** — the same threads that deliver
  entry, item and topic events to clients.
- Each disconnect logs `Destroying ...` at INFO (`ClientEndpointManagerImpl#removeEndpoint`).
- `ClientEndpointImpl#destroy` → `OperationParkerImpl#onClientDisconnected` walks **every wait set**
  for **each** disconnected client: O(clients × wait sets) under churn.
- `ClientLifecycleMonitor` runs `getClientsInCluster()`, a blocking invocation on every member, on
  the shared scheduled executor.
- Authentication of new connections runs on the generic/urgent operation threads, and with TLS each
  short-lived connection also costs a full handshake on an I/O thread.

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

lp.getConnectionBufferConfig()
        .setPooled(true)                                     // borrow buffers only while in use, §4.4.2
        .setClientBufferBytes(32 << 10)                      // per buffer, client connections
        .setMaxPooledBytes(2L << 30);                        // member-wide cap on connection buffers

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
        .setInMemoryFormat(InMemoryFormat.BINARY)
        .setImmutable(true);                                 // insert-only, §4.2

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

#### 4.2.1 Semantics: insert-only

A map with `setImmutable(true)` accepts the insert of a key that is not present and refuses every
operation that would change, remove or insert again an existing key. Refusals throw
`ImmutableEntryException`, which extends `IllegalStateException`, so official clients surface it
as `IllegalStateException` with the message (no new client error code).

| Operation | Key absent | Key present |
|---|---|---|
| `put`, `set`, `putAsync`, `setAsync`, `putAll`, `setAll`, `putTransient` | inserted | **refused** (duplicate insert) |
| `putIfAbsent`, `putIfAbsentAsync` | inserted | returns the existing value, nothing changes (its contract already rejects the insert) |
| `replace`, `merge`, `compute*`, entry processors that call `setValue`, `MapLoader#loadAll(replaceExisting=true)` | as for an insert, where the operation inserts | **refused** |
| `remove`, `delete`, `evict`, `evictAll`, `clear`, `removeAll`, `setTtl` | — | **refused** |
| `get`, `getAll`, `containsKey`, queries, `values`, `entrySet`, read-only entry processors | unchanged | unchanged |
| `MapLoader` load on a miss | inserted | — |
| `destroy` (the whole map) | allowed: an administrative action that drops the map |

Checks run on the partition owner inside the operation, so two concurrent inserts of the same key
are ordered: exactly one succeeds, the other is refused. A refused insert leaves the stored value
untouched, whether or not the bytes are equal.

A client that retries a `put` after a timeout can be refused for its own first attempt that did
succeed. Applications that retry should treat `ImmutableEntryException` as "already inserted" or
use `putIfAbsent`, which returns the stored value. The user guide says so.

#### 4.2.2 Restrictions validated at start

`ConfigValidator` refuses an immutable map that:

- is not `BINARY` (until the optional phase 3 adds `NATIVE` and tiered);
- has eviction, a time-to-live or a max-idle time (entries are never removed);
- has a merge policy other than "existing wins" (§4.2.3).

#### 4.2.3 Replication, recovery and split brain (FR6, NFR availability)

Unchanged mechanisms: `backup-count`/`async-backup-count`, partition migration, anti-entropy.
Since values never change, backups hold the same bytes as the primary and anti-entropy only ever
adds missing entries.

- **Member failure:** backups are promoted as today; no acknowledged insert is lost while at least
  one replica survives.
- **Loss of every replica of a partition:** its entries are gone. The member keeps no record of
  them (no tombstones), so their keys can be inserted again. The partition-lost event says which.
- **Split brain:** both sides may insert the same key. On merge the existing entry wins; when the
  values differ, the conflict is counted (`immutableMergeConflicts`) and logged with the map and
  key, so the application can find it.

### 4.3 Shared payloads and the no-copy invariant (FR4, NFR memory)

#### 4.3.1 What exists and what is added

On `BINARY` the stored `HeapData` array is already the transmission-ready representation, and the
response frame references it (§3.1). Immutability makes sharing it safe: the array can never
change while a response is being written. Phase 1 adds:

- `SharedPayload` (core, `--release 17`): a small object per hot record that holds the record's
  array (no copy) and a reference count of outbound responses. It is created lazily on the first
  read under load protection and kept with the record. It exists so the member can count
  **physical** outbound bytes (§4.4.1) and report references (§4.7).
- `ClientMessage.Frame` gets a frame kind that refers to a `SharedPayload`. `DataCodec#encode`
  uses it when the `Data` comes from an immutable map. `MapGetCodec`, `MapGetAllCodec` and the
  paged entry/value codecs go through `DataCodec`, so they are covered without codec changes. The
  bytes on the wire are identical.
- `ClientMessageWriter` writes such a frame chunk by chunk into the connection's send buffer, like
  it does for arrays today. A reference is taken when a response is queued and released when its
  last byte is written, or when the connection closes (the close path releases every queued
  frame — covered by a dedicated test).

#### 4.3.2 The invariant and its test

**Allocated bytes per `get` of an immutable map do not depend on the value size.** An allocation
test (§6) reads 1 KB, 100 KB and 1 MB values from one member and fails if the bytes allocated per
request grow with the size. Any future change that copies the value per request (a new codec path,
an interceptor, statistics) fails this test.

What remains per request: one `ClientMessage`, a few small frames, one reference count increment,
and the bounded, per-connection encryption into the TLS buffer.

#### 4.3.3 Reads between members (measured: 3–5 copies, §3.1)

A read that a member forwards to the key's owner — an embedded member reading through its own
proxy, **which is how our services read**, or a client request that reached a non-owner — copies
the value 3–5 times today. For immutable maps, Phase 1 removes those copies, all on the member side
and in this order of value:

1. **Member-side near cache by default.** An immutable map's entries never change and are never
   removed (§4.2.1), so a near cache on the members needs no invalidation and can never be stale.
   Under load protection, immutable maps get an `OBJECT` near cache on each member (bounded by
   entry count/size, LRU), which takes the measured path from 4 copies to **0**. Hazelcast 5.7 can
   be configured this way today (§7). Members return the cached instance to every caller, so callers
   must not modify it; for an immutable map that is the contract (documented).
2. **Single flight.** Concurrent misses of the same key on a member share one remote invocation,
   so a burst of 50,000 reads costs one transfer from the owner, not 50,000. Measured on 5.7's near
   cache: 48 of 256 concurrent first reads went to the owner (§3.1).
3. **Deserialise a response once.** `InvocationFuture#resolve` keeps the deserialised response
   instead of reading the packet again for every waiting stage (the fifth copy). This is a plain
   fix that helps every map, not only immutable ones.
4. **One copy on the wire path.** The owner writes the response packet's value bytes straight from
   the record's array (a packet with a shared frame, like §4.3.1) instead of serialising them into
   a new array, and the receiver keeps the packet's array as the `Data` of the response instead of
   copying it out again.

With 1 and 2 a member holds at most one copy of each hot value it does not own, however many
readers it has. 3 and 4 shrink the path for the reads that still go remote (cold keys).

### 4.4 Bounded buffers and back pressure (NFR back pressure, NFR memory, FR5)

#### 4.4.1 Accounting

- Per connection: logical pending bytes and messages (exists, made observable and limited);
  in-flight requests (decoded, not yet answered); connection buffers held.
- Per member: logical pending bytes; **physical** pending bytes — a `SharedPayload` adds its size
  when its outbound reference count goes 0 → 1 and subtracts it on 1 → 0, so 50,000 queued
  responses of one 1 MB value count 1 MB; in-flight client requests; connection buffer bytes.

#### 4.4.2 Connection buffers (largest fixed cost with TLS)

1. **Pooled, borrowed on demand.** Client connections stop owning their four buffers. They borrow
   them from a member-wide pool while there is something to read, decode, encode or encrypt, and
   give them back when the pipeline is idle (no partial frame in the inbound buffers, nothing
   queued outbound). An idle or waiting connection then holds no buffers. The TLS engine itself
   stays per connection (it holds the session), its buffers do not.
2. **Sized for clients.** Client buffers default to 32 KB under load protection (member-to-member
   connections keep 128 KB). TLS records are at most ~16 KB, so throughput per connection is
   unaffected at our message sizes; Phase 0 verifies.
3. **Capped.** `maxPooledBytes` bounds the pool. When it is exhausted, connections wait for a
   buffer (their reads are deferred, step 1 of §4.4.3) instead of allocating more.

Estimate at 50,000 TLS connections: today ~26 GB; with 32 KB buffers held only by the connections
that are active at that moment, the pool cap (e.g. 2 GB) is the bound.

#### 4.4.3 Actions, cheapest first

1. **Defer.** When a connection is over `maxPendingBytesPerConnection` or
   `maxInFlightRequestsPerConnection`, or no pooled buffer is free, the member stops reading from
   that connection (removes read interest from its inbound pipeline) until it drains below a low
   watermark. TCP flow control then pushes back on the client. Works with every client.
2. **Throttle.** When the member is over `maxInFlightRequests` or `maxPhysicalPendingBytes`, it
   stops reading from client connections in round robin, keeping member-to-member connections and
   the maintenance lane running.
3. **Reject.** A request that is decoded while the member is above the hard watermark, or while heap
   occupancy after the last GC exceeds `heapRejectThreshold`, is answered with `HECATON_OVERLOAD`
   (`HecatonOverloadException`, error code 20, known to official clients). Reads of large values
   (above a size threshold) are rejected before small ones.
4. **Disconnect.** A connection whose outbound queue has not drained for
   `slowConsumerTimeoutSeconds` is closed; its queued frames and buffers are released.

Also bounded: authenticated client messages (`maxMessageBytes`, checked from the frame header
before allocating), and client-originated operations per partition thread (the
`BackpressureRegulator` idea applied to client requests).

#### 4.4.4 Hot keys and TLS cost

With smart routing every get of a hot key goes to its partition owner, so that member encrypts and
sends all of it: 50,000 × 1 MB is 50 GB of TLS output from one member, whatever we do with memory.
The design keeps that member's memory bounded (shared payload, pooled buffers, deferral) and
spreads the encryption over all its I/O threads; the time to serve all readers is then bound by
its network and CPU, and other members are not affected. Phase 0 measures how long a 50,000-reader
burst takes. Spreading one hot key's client reads over several members would need a client-side
change, which is out of scope (server side only, §1.6).

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
   generic operation threads to the maintenance lane's auth pool. TLS handshakes are counted
   against the same limits, and TLS session resumption is enabled for clients so reconnecting
   clients skip the full handshake.
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
map is needed.

#### 4.6.2 Transparent routing (FR9)

Official clients keep sending `QueueOffer`, `QueuePoll`, ... to the partition of `jobs`. The
member's message task sees a sharded queue and routes on the member:

- `offer`: to the shard owned by the local member if there is one with room, else round robin over
  shards weighted by their sizes.
- `poll`/`take`: see §4.6.4; a blocking `poll`/`take` registers one member-local waiter that is
  woken by an offer to any shard, with the caller's timeout.
- `size`, `isEmpty`, `drainTo`, `iterator`, `contains`: fan out to all shards and combine.
- Item listeners: registered on every shard.

The extra hop (client → routing member → shard owner) is the price of transparency. Phase 4 adds an
optional smart path for Hecaton's Java client that offers directly to a shard.

#### 4.6.3 Shard state

Each shard reports its size and the creation time of its head item (`QueueItem` already records
its creation time). Routing members keep this per logical queue, refreshed from every shard
response and at least every 50 ms.

#### 4.6.4 Ordering: FIFO per shard, best effort across shards

- Within a shard, FIFO holds as today.
- Across shards, a `poll` takes from the shard whose head is **oldest** according to the routing
  member's shard state, falling back to the next oldest if that shard turned out empty. Items are
  therefore taken approximately in insertion order; the error is bounded by the state refresh
  interval plus clock skew between shard owners. No guarantee is given, and the user guide says so.
- Queues that need strict global FIFO keep `shardCount = 1`.

#### 4.6.5 Migrating from client-side hashing (FR10)

`QueueConfig#setShardNames("jobs-0", ..., "jobs-7")` declares existing physical queues as the shards
of the logical queue, without moving data. Old clients that hash themselves keep using `jobs-i`
directly; new clients use `jobs`. Both see the same items. Once all clients are moved, the client
hashing code can be removed.

#### 4.6.6 Failure behaviour

Each shard is a normal queue: acknowledged items are on `1 + backup-count` members; when a member
fails, its shards' backups are promoted. A waiting `take` on the routing member survives the loss
of a shard owner; if the routing member itself fails, the client retries as it does today.

### 4.7 Observability

Metrics in the existing registry (JMX, Prometheus exporter), prefix `loadProtection`:

| Area | Metrics |
|---|---|
| Connections | active, opened/s, closed/s, refused (by reason), authentications and TLS handshakes in progress / queued, TLS session resumptions |
| Connection buffers | pool bytes in use / capacity, connections waiting for a buffer |
| Queued bytes | outbound logical bytes and messages (member, top-N connections), outbound physical bytes, connections with reads suspended |
| References | shared payloads alive, references held |
| Partition load | operations and queue depth per partition thread, hot keys (top-N by reads, sampled), non-owner reads |
| Replication lag | pending backup operations, async backup lag, anti-entropy syncs |
| Rejections | by reason: overload, heap, message too large, slow consumer disconnect, immutable violation; immutable merge conflicts |
| Cleanup backlog | maintenance queue length, age of oldest task, disconnect batches, wait-set scan time |
| Queues | per shard size, head age, offer/poll rate, routing hops |

### 4.8 Compatibility and rollout

- Wire protocol unchanged; official clients need no change.
- Off by default; with it off nothing changes (verified by the inherited suite).
- Mixed-version clusters: enabling the feature requires every member on the version that has it
  (cluster version gate, F14); `isImmutable`/`shardCount` are in config equality.
- `docs/changes-from-hazelcast.md` gets the new config and exception; `docs/load-protection.md`
  is the user guide.

### 4.9 Alternatives considered

- **Only lowering the socket buffer properties.** `SOCKET_CLIENT_*_BUFFER_SIZE` exist today and are
  a useful stop-gap (❓ §7), but they still allocate per connection whether busy or idle; pooling
  bounds the total.
- **Client-side caching only (near cache).** Reduces repeat reads but does not bound the server when
  50,000 clients start cold at the same moment; still useful alongside.
- **Copying into pooled direct buffers per request.** Bounded allocation but bandwidth and pool size
  still follow readers; sharing the stored array avoids the copy.
- **A new queue type instead of sharding `IQueue`.** Would need new codecs and client support;
  sharding keeps the `IQueue` API and the official clients.

## 5. Phases

| Phase | Content | Exit criteria | Est. |
|---|---|---|---|
| **0 Measure** — allocation part done | Done: `HotReadAllocation` + `tools/perf/hot-read-allocation.sh` — copies per read by path on Hazelcast 5.7 and Hecaton (§3.1). Open: the perf rig at 50k readers, slow readers (retention), connection churn with p99 of unrelated map/queue ops, time to serve a 50k-reader burst | §3.1 measured (done); the rest in `docs/performance.md` | 1 ew left |
| **1 Immutable maps + bounds** (mandatory first) | `LoadProtectionConfig` + toggle; insert-only maps; first, reads between members — our path (§4.3.3: member near cache, single flight, deserialise once, one copy on the wire); then `SharedPayload`, shared frames and the no-copy invariant test; pooled, capped connection buffers; outbound accounting and limits; admission (defer/throttle/reject/disconnect); message size limit; metrics for these | `hot-read-allocation.sh` with `--max-copies-per-read 0` passes for every client and member-caller scenario of an immutable map (1 for cold remote reads); 50k × 1 MB reads over TLS keep heap growth below a fixed bound; 50k idle TLS connections cost less than the pool cap; no OOM with stalled readers; toggle off = inherited suite unchanged | 8–10 ew |
| **2 Isolation** (mandatory) | Maintenance lane, batched disconnects, accept/auth/handshake limits, TLS session resumption, quiet churn, cleanup metrics | Under a churn of N connections/s (N from Phase 0), p99 of map/queue ops within X% of the no-churn baseline; cleanup backlog bounded and drained | 3–4 ew |
| **3 Other formats** (optional) | `NATIVE` and tiered immutable maps sharing native blocks | Only if needed | 2–3 ew |
| **4 Partitioned queue** (nice to have) | `shardCount`, member-side routing, oldest-head polling, blocking take across shards, `shardNames` migration, optional smart client path | Throughput of one logical queue scales with members up to a measured limit; no acknowledged item lost when a member is killed; ordering error measured and documented | 4–6 ew |
| **5 Hardening and docs** | Declarative/Spring config, user guide, chaos/soak tests, defaults from measurements | Soak 24 h at design peak with no heap growth trend | 2 ew |

Phases 1 and 2 are independent after Phase 0 and can run in parallel. Each phase is one PR into
`feature/m11-load-protection`, following CONTRIBUTING (tests, multi-member and failure tests,
DCO, fork log entry).

## 6. Testing

- **Unit:** `SharedPayload` reference counting (including close-while-queued), shared-frame writing
  across partial writes and through `TlsEncoder`, the insert-only operation matrix (§4.2.1) for
  every map operation and entry processor kind, concurrent inserts of one key (exactly one wins),
  buffer pool borrow/return and exhaustion, limits and watermarks, disconnect batching.
- **No-copy invariant (CI):** `HotReadAllocation --max-copies-per-read 0` for the client and
  member-caller paths of an immutable map (exit status 1 when exceeded), and allocated bytes per
  `get` for 1 KB, 100 KB and 1 MB values, which must not grow with the size.
- **Connection buffers (CI):** 5,000 idle TLS connections; member heap held by connection buffers
  stays under the pool cap.
- **Multi-member:** inserts and reads during migration and member loss; partition loss (keys can be
  inserted again); split-brain merge with conflicting inserts (existing wins, conflict counted);
  partitioned queue offers/takes while a shard owner is killed — no acknowledged item lost, none
  duplicated beyond the existing at-least-once rules.
- **Back pressure:** stalled clients (stop reading the socket) — member heap stays bounded, the
  slow consumer is disconnected, other clients unaffected; overload rejections counted.
- **Isolation:** TLS churn generator alongside a steady map/queue workload; asserts the p99 bound
  and a bounded maintenance backlog.
- **Toggle:** feature off — inherited suites unchanged (CI as today); feature on — the map and
  queue suites run again with load protection enabled (nightly), with immutable-map tests in place
  of the update/remove tests that no longer apply.
- **Performance/soak:** Phase 0 rig at 50k concurrent TLS clients; 24 h soak in Phase 5.
- **Compatibility:** official Java client (and one non-Java client from `compat-tests/`) against a
  member with the feature on; the wire is unchanged.

## 7. Open questions

Resolved in the reviews: see §1.6.

- ❓ **Stop-gaps on Hazelcast 5.7, server side, configuration only:**
  - A near cache on the members, `OBJECT` format, sized for the hot set (measured: 4 copies → 0
    once it holds the value, §3.1; the first concurrent burst of a key still goes to the owner).
    Callers then share the cached instance and must not modify it.
    ```java
    config.getMapConfig("catalog").setNearCacheConfig(new NearCacheConfig()
            .setInMemoryFormat(InMemoryFormat.OBJECT)
            .setEvictionConfig(new EvictionConfig()
                    .setEvictionPolicy(EvictionPolicy.LRU)
                    .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT)
                    .setSize(1_000)));               // hot values per member: 1,000 × 1 MB = 1 GB
    ```
  - For connection churn: `hazelcast.socket.client.receive.buffer.size` and
    `hazelcast.socket.client.send.buffer.size` (KB, default 128) to 32 cut the buffers of each
    client connection by about 4× (§3.2).
- ❓ **Default limits.** Placeholders in §4.1 until the rest of Phase 0 is in.
- ❓ **Names.** `LoadProtectionConfig`, `MapConfig#setImmutable`, `QueueConfig#setShardCount`.

## 8. As implemented

The user guide is [load-protection.md](../load-protection.md). Phases 1, 2 and 4 are in, with
XML/YAML support from phase 5; phase 3 (`NATIVE`/tiered immutable maps) is not. Where the code
differs from §4:

- **Queue ordering.** Offers go to the emptiest shard and polls take from the fullest, using
  size hints refreshed in the background, instead of polling the shard with the oldest head
  (§4.6). It needs no timestamps in the items and keeps the shards level; the order across
  shards is best effort, as agreed in §1.6. `maxSize` applies to each shard.
- **Buffer pool cap.** `maxPooledBytes` caps the idle buffers kept for reuse. A connection that
  needs a buffer when the pool is empty allocates one; reads never wait for a buffer.
- **TLS buffers.** On the unified port the TLS buffers are sized before the protocol is known, so
  only the socket-facing buffers of client connections are pooled; the TLS engine's own buffers
  are not.
- **TLS session resumption** (phase 2) is not implemented; the accept and authentication limits
  bound the handshake cost instead.
- **In-flight counting** covers requests that run as partition operations (map and queue
  calls); other client requests are bounded by the per-connection read pause only.
- **Reads between members.** The member near cache of an immutable map is a single-flight cache
  in `NearCachedMapProxyImpl`, sized by `immutableMapNearCacheSize`; `InvocationFuture` keeps the
  `Data` it reads from a response so that each waiting stage does not deserialize again.
- **Spring** configuration is not done; Spring users set the fields on the `Config` bean.

Not done from §5's exit criteria: the 50k-reader, churn-p99 and 24 h soak runs on the perf rig.
The unit and multi-member tests (`ImmutableMapTest`, `LoadProtectionNetworkTest`,
`MaintenanceLaneTest`, `ShardedQueueTest`, `LoadProtectionDeclarativeConfigTest`) cover the
behaviour, and `hot-read-allocation.sh` measures copies per read.
