# Load protection (F23)

Load protection keeps members responsive when many clients read the same large values, when
clients connect and disconnect at a high rate, and when one queue carries most of the traffic. It
is a Hecaton feature; Hazelcast has no equivalent. It is **off by default**, and with it off the
member behaves exactly as before.

The design, the measurements behind it, and where it deviates from the plan are in
[design/load-protection.md](design/load-protection.md).

## Turning it on

Members are usually started from Java code:

```java
Config config = new Config();
config.getLoadProtectionConfig().setEnabled(true);
config.addMapConfig(new MapConfig("prices").setImmutable(true)
        .setInMemoryFormat(InMemoryFormat.BINARY));
config.addQueueConfig(new QueueConfig("jobs").setShardCount(8));
HecatonInstance member = Hecaton.newHecatonInstance(config);
```

The system property `hecaton.load-protection.enabled` (`ClusterProperty.LOAD_PROTECTION_ENABLED`)
overrides the configured value either way, without a code change. Every member of a cluster
should use the same setting.

Declarative configuration has the same settings:

```yaml
hecaton:
  load-protection:
    enabled: true
  map:
    prices:
      immutable: true
  queue:
    jobs:
      shard-count: 8
```

```xml
<load-protection enabled="true"/>
<map name="prices"><immutable>true</immutable></map>
<queue name="jobs"><shard-count>8</shard-count></queue>
```

## What it does

### Immutable maps

`MapConfig#setImmutable(true)` makes a map immutable: the value of a key it holds is never
updated. `put`, `set`, `putIfAbsent` and `putAll` of a new key work, with or without a
time-to-live or max-idle. Any operation that would update the value of a held key (`put`, `set`
or `putAll` of a held key, `replace`, an entry processor that sets a held entry's value,
`loadAll` with `replaceExistingValues`, and these in transactions) fails with
`ImmutableEntryException`. A duplicate insert is rejected, not ignored; `putIfAbsent` of a
held key returns the held value as usual.

Entries can still go away: `remove`, `delete`, `evict`, `evictAll`, `clear`, entry processors that
remove an entry, `setTtl`, and the map's own time-to-live, max-idle and eviction policy (LRU, LFU,
...) all work as on any map. Once a key is no longer held it can be inserted again, possibly with
a different value. During a split-brain merge the held value wins. An immutable map must use
`InMemoryFormat.BINARY`; the member refuses another format.

Because held values are never updated, members can share them instead of copying them:

- **Reads from embedded members** go through a small near cache per map, of
  `immutable-map-near-cache-size` entries (1000 by default; `0` turns it off). Concurrent misses
  for one key make one remote call (single flight); absent keys are never cached. Repeated reads
  of a hot value then allocate nothing. Removals, expiry and inserts invalidate it like any near
  cache: invalidations are sent in batches (within `hecaton.map.invalidation.batchfrequency.seconds`,
  10 s by default), so until one arrives a member can still return the last value of a key that was
  removed or expired elsewhere.
- **Values sent to clients and members** are written to the socket from the stored bytes, not
  copied into each response. The per-response cost is a small header, whatever the value's size.

Clients need no change. With smart (all-member) routing each read goes straight to the owner,
which sends the stored bytes.

### Bounded buffers and back pressure

- **Outbound:** each client connection may queue at most `max-pending-bytes-per-connection`
  (64 MB) of responses, and a member at most `max-physical-pending-bytes` (1 GB) in total. A
  value shared by many responses is counted once. Above a limit the member stops reading new
  requests from that connection (or from all client connections) until the queue drains; a
  client that reads nothing for `slow-consumer-timeout-seconds` (30) is disconnected.
- **Admission:** a client connection may have `max-in-flight-requests-per-connection` (1024)
  requests in progress and the member `max-in-flight-requests` (100,000). Above those limits, or
  when the heap after the last garbage collection is above `heap-reject-threshold` (90%) of the
  maximum, requests fail fast with `HecatonOverloadException`. Clients do not retry it: the
  application receives it, and should back off and try again.
  Client messages larger than `max-message-bytes` (64 MB) close the connection.
- **Connection buffers:** client connections borrow their read and write buffers
  (`client-buffer-bytes`, 32 KB) from a member-wide pool while they have data to move, so an idle
  connection holds none; the pool keeps at most `max-pooled-bytes` (1 GB) of idle buffers. These are
  the buffers in the heap; the kernel's socket buffers keep their configured size
  (`hecaton.socket.client.receive.buffer.size` / `send.buffer.size`, 128 KB by default). Kernel
  socket buffers smaller than one TCP segment can stall connections (see
  [below](#socket-buffers-and-tcp-stalls)).

### Connection churn on its own threads

Cleanup after a client disconnects (endpoint removal, cancelling its waiting operations, listener
removal) and authentication of new connections run on a separate maintenance lane of
`thread-count` (2) threads, so a burst of connects and disconnects does not delay map and queue
operations. Disconnects are processed in batches. `max-accepts-per-second` (0, unlimited) caps
new client connections and `max-concurrent-authentications` (256) the authentications in
progress; above either, new connections are refused and the client retries.

### Sharded queues

`QueueConfig#setShardCount(n)` spreads a queue over `n` physical queues on different partitions,
so its load is shared by several members. Clients and members use the queue by its name as
before; the member that receives a call routes it to a shard. Offers go to the emptiest shard and
polls take from the fullest, so the queue is FIFO within a shard and only roughly FIFO overall.
`maxSize` applies to each shard; `addAll`, `drainTo` and the bulk removals are not atomic across
shards; item events carry the logical queue's name.

`QueueConfig#setShardNames(List)` makes existing queues the shards instead, without moving their
items. An application that already spreads items over `jobs-0` … `jobs-7` itself can keep that
code running while new code uses the logical queue `jobs`; both see the same items.

## Settings

| Setting | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Turns load protection on |
| `immutable-map-near-cache-size` | 1000 | Entries of each immutable map cached on every member |
| `outbound.max-pending-bytes-per-connection` | 64 MB | Responses queued per client connection before its reads pause |
| `outbound.max-physical-pending-bytes` | 1 GB | Responses queued on the member before all client reads pause |
| `outbound.slow-consumer-timeout-seconds` | 30 | A client that reads nothing for this long is disconnected |
| `admission.max-in-flight-requests-per-connection` | 1024 | Requests in progress per client connection |
| `admission.max-in-flight-requests` | 100,000 | Client requests in progress on the member |
| `admission.max-message-bytes` | 64 MB | Largest client message accepted |
| `admission.heap-reject-threshold` | 0.9 | Heap use after GC above which requests are refused; 0 turns it off |
| `maintenance.thread-count` | 2 | Threads of the maintenance lane |
| `maintenance.max-backlog` | 100,000 | Tasks queued on the lane; above it they run on the caller |
| `maintenance.max-accepts-per-second` | 0 | New client connections per second; 0 is unlimited |
| `maintenance.max-concurrent-authentications` | 256 | Authentications in progress |
| `connection-buffers.pooled` | `true` | Borrow the read and write buffers of client connections from the pool |
| `connection-buffers.client-buffer-bytes` | 32 KB | Size of a client connection's read and write buffers in the heap (not the kernel's socket buffers) |
| `connection-buffers.max-pooled-bytes` | 1 GB | Idle buffers kept for reuse |

In Java the groups are `getOutboundConfig()`, `getAdmissionConfig()`, `getMaintenanceConfig()`
and `getConnectionBufferConfig()` of `LoadProtectionConfig`.

## Metrics

Under the `loadProtection` prefix:

| Metric | Meaning |
|---|---|
| `clientRequestsInFlight` | Client requests in progress |
| `overloadRejections` | Requests refused with `HecatonOverloadException` |
| `readSuspensions` | Times reading from a client connection paused for back pressure |
| `slowConsumerDisconnects` | Clients disconnected for not reading |
| `outboundPhysicalBytes` | Bytes queued to clients, a shared value counted once |
| `outboundSharedLogicalBytes` | Bytes of shared values as the responses see them |
| `connectionBufferBytesInUse`, `connectionBufferBytesIdle`, `connectionBufferAllocations` | The buffer pool |
| `maintenanceBacklog`, `maintenanceTasks`, `maintenanceTasksRunInline` | The maintenance lane |
| `disconnectBatches`, `disconnectedClients` | Batched disconnect cleanup |
| `authenticationsInProgress`, `clientConnectionsAccepted`, `clientConnectionsRefused` | New connections |
| `immutableRejections`, `immutableMergeConflicts` | Changes refused on immutable maps |

## Socket buffers and TCP stalls

Keep the kernel's socket buffers for client connections at least as large as two TCP segments
when load protection is on. In a test with 64 TLS clients on the member's host and 50,000 reads
of a 1 MB value in flight, kernel buffers of 32 KB (one loopback segment is 64 KB) stalled
connections within a minute: the member stopped reading a connection at its outbound limit, its
kernel held one full segment for the client, the client had read everything that had arrived,
and neither side sent again until the connection was closed as a slow consumer. With the
inherited 128 KB buffers the same test completed. Load protection therefore leaves the kernel's
buffers alone (`hecaton.socket.client.receive.buffer.size` and `send.buffer.size` set them);
`client-buffer-bytes` sizes only the buffers in the heap. Over Ethernet a segment is about 1.5 KB
(9 KB with jumbo frames), so the risk is for clients on the member's host or on networks with
large segments.

Hazelcast 5.7 with 32 KB client socket buffers and without TLS completed the same test: it does
not pause reads.

## Measuring copies

`tools/perf/hot-read-allocation.sh` runs the `HotReadAllocation` benchmark under Flight Recorder
and reports how many copies of a value each read allocates, by caller (smart client, short-lived
client, embedded member), format and near cache. See
[performance.md](performance.md#hot-reads-of-a-large-value).
