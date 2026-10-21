# CP Subsystem and CPMap

Hecaton includes the CP Subsystem (F7) and `CPMap` (F8) in the core jar. The CP Subsystem
keeps its data structures in Raft groups, so they stay consistent through network partitions
and member failures, as long as a majority of each group's members is reachable.

The engine and the `IAtomicLong`, `IAtomicReference`, `FencedLock`, `ISemaphore` and
`ICountDownLatch` implementations come from Hazelcast 5.4.0, the last release in which they
were open source (see [`provenance.md`](provenance.md)), adapted to the 5.7.0 APIs. `CPMap` is
Hecaton's own implementation.

## Enabling it

```yaml
hecaton:
  cp-subsystem:
    cp-member-count: 3        # 0 (default): CP structures run in UNSAFE mode, without Raft
    group-size: 3
    session-time-to-live-seconds: 60
    missing-cp-member-auto-removal-seconds: 14400
    fail-on-indeterminate-operation-state: true
    maps:
      orders:
        max-size-mb: 50
        purge-enabled: true
```

With `cp-member-count` set, the first that many members form the CP Subsystem. Names select
a group: `getMap("orders@payments")` uses the `payments` group, `getMap("orders")` the default
group. Clients use the same API through their `CPSubsystem`.

### Exactly-once operations

When a leader changes while an operation is in flight, the caller cannot always tell whether
it was committed. With `fail-on-indeterminate-operation-state: false` (the default, as in
Hazelcast) the operation is retried, and a committed one may then be applied twice: an
`addAndGet(1)` may add 2, a `put` may return its own value as the previous one. With `true`
the caller gets an `IndeterminateOperationStateException` instead and decides what to do.
Use `true` when you need linearizable, exactly-once behaviour.

## CPMap

`CPMap` is a key-value map replicated by Raft. It is not partitioned: every member of its CP
group holds the whole map, so it must fit in memory.

| Method | Effect |
|---|---|
| `put(k, v)` | Sets the value and returns the previous one |
| `set(k, v)` | Sets the value; nothing is returned, so the previous value is not sent back |
| `putIfAbsent(k, v)` | Sets the value if the key is absent; returns the current value, or null |
| `get(k)` | Linearizable read |
| `remove(k)` / `delete(k)` | Removes the key; `remove` returns the previous value |
| `compareAndSet(k, expected, v)` | Sets the value if the current one equals `expected` (by serialized form) |

Keys and values must not be null.

### Size limit

`max-size-mb` (default 100, at most 2000) limits the serialized size of a map's keys and
values. A write that would make the map larger fails with `IllegalStateException`, and the
map is not changed.

### Purge

With `purge-enabled: true`, entries that have not been written for a while can be removed:

```java
CPMapPurgeResponse r = member.getCPSubsystem().getCPDataStructureManagementService()
        .purgeCPMap("orders", Duration.ofDays(30)).toCompletableFuture().get();
r.purgedCount();                     // entries removed
r.oldestRemainingEntryTimestamp();   // epoch millis of the oldest remaining write, -1 if none remain
```

Each entry carries the time of the operation that last wrote it. Purge is available on
members; the client protocol has no message for it.

### Permissions

With security enabled, clients need a `cpmap` permission: `create`, `destroy`, `read`
(`get`), `put` (`put`, `set`, `putIfAbsent`, `compareAndSet`) and `remove` (`remove`,
`delete`).

## Administration

The CP Subsystem management API (`getCPSubsystemManagementService()`), the REST endpoints
under `/hecaton/rest/cp-subsystem` and the `hc-cluster-cp-admin` script promote, remove and
reset CP members and list groups and sessions. Management Center's CP messages are served too.

## Testing

Besides the ported 5.4.0 CP test suite, `CPLinearizabilityTest` runs CPMap and IAtomicLong
operations from members and a client while a nemesis partitions the Raft traffic, isolates the
leader and replaces CP members, and checks the recorded history with a Porcupine-style
linearizability checker. Set `-Dhecaton.test.linearizability.seconds=...` for longer runs.

## Limitations

- CP persistence (F9) is not available yet: a restarted CP member starts empty and must be
  removed and replaced (planned for M9). `persistence-enabled: true` is refused at startup.
- `CPMap` has no size, iteration or listener methods, matching the API.
