# Persistence (F10) — design

`uk.org.hecaton:hecaton-persistence` is a member module (Java 25) that implements Persistence
(Hot Restart in Hazelcast): IMap and ICache data, and the cluster metadata needed to bring a
cluster back with the same partition table, survive a restart of all members. It is Hecaton
code, written from the public documentation of the feature; the configuration
(`PersistenceConfig`, `DataPersistenceConfig`), the public API (`PersistenceService`) and the
management operations (force start, partial start, backup) are the ones core already has.

## On disk

```
<base-dir>/<member-dir>/            one directory per member, locked while the member runs
    member                          member UUID, address, cluster name
    cluster/                        cluster metadata (members, partition table, state, version)
    structures                      structure ids: (id, kind, name), append-only
    data/p<partition>/<id>.chunk    one log per partition
```

A member started with a `base-dir` that holds directories of earlier members claims the first
one it can lock (a `FileChannel` lock on `<member-dir>/lock`), so several members can share a
`base-dir`; without one it creates a new directory.

## The log-structured store (F10.1)

Every partition has its own log: an ordered list of chunk files. All replicas of a partition on
a member (owner and backups) share it, and all writes to it are made by the partition's thread,
so a log needs no locking.

- **Records** are `[crc32c][length][type, seq, structure, key, value]`, with types `PUT`,
  `REMOVE` (a tombstone) and `CLEAR` (a prefix tombstone: every earlier record of the
  structure). `seq` grows by one per record in a partition. The value is opaque to the store;
  the map and cache layers put their record metadata in front of the serialized value.
- **Chunks** start with a header (magic, format version, chunk id, and the id of the oldest chunk
  it supersedes). The active chunk is appended to; at `max chunk size` it is closed (and forced
  to disk) and a new one started.
- **Index**: per structure, the key → (chunk, offset, size, seq) of its live record, on the heap.
  It is rebuilt by reading the log at start and is used for garbage accounting and compaction.
- **Compaction** runs when the closed chunks hold more garbage than live data (and at least two
  chunks' worth). The live records of all closed chunks are written, in `seq` order, to a new
  file that supersedes them; it gets the id of the newest closed chunk and replaces it with an
  atomic rename, then the older chunks are deleted. Tombstones are dropped, since no older
  record survives compaction. A crash between the rename and the deletion is repaired at the
  next start (the superseded chunks are deleted). Chunks therefore stay ordered by `seq`, and
  reading the chunks in order replays the history.
- **Durability**: a write is in the operating system's cache when the operation returns, so it
  survives the failure of the process. With `fsync` on the structure's `DataPersistenceConfig`
  the chunk is forced to disk before the operation returns, so it also survives the failure of
  the machine.
- **Torn writes**: a record whose length runs past the end of the active chunk, or whose checksum
  does not match, ends the log: the chunk is truncated there. A bad record in a closed chunk
  means the data is corrupted, and the member refuses to start from it.

## Map and cache data (F10.2)

A record store of a structure with data persistence enabled gets a mutation observer that writes
every put, update, removal, eviction, clear and destroy to the partition's log. Data arriving by
migration or replication is written the same way, and a replica leaving the member (migration)
clears it from the log. At start, after the cluster has validated the restored partition table,
each member reads its logs (with `parallelism` threads) into the record stores of the
partitions it holds.

## Cluster metadata and restart (F10.3, F10.4)

A member writes (atomically: temporary file, then rename) its UUID, the member list, the
partition table, the cluster state and the cluster version whenever they change. A restarted
member reuses its UUID. The master waits up to `validation-timeout-seconds` for the members of
the persisted member list, checks that they agree on the partition table, installs it (with the
members' current addresses), and asks the members to load their data; operations are retried
until loading completes on all members (`data-load-timeout-seconds`). If members are missing or
disagree, `cluster-data-recovery-policy` decides: `FULL_RECOVERY_ONLY` fails the restart, the
partial policies start with the members holding the most recent (or most complete) partition
table and exclude the others, which delete their data and join as new members. Force start
deletes the data of all members and starts the cluster empty.

## Backup (F10.5)

`PersistenceService#backup()` copies, on every member, the closed chunks (hard links where the
file system allows) and a copy of the active chunk's written part, plus the metadata, into
`<backup-dir>/backup-<seq>/<member-dir>`. A backup is restored by using it as `base-dir`.
