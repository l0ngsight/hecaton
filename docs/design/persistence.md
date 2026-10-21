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
    data/structures                 structure ids: (id, kind, name), append-only
    data/configs/<id>               configurations of caches, to recreate them
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
  it supersedes). The active chunk is appended to; at `hecaton.persistence.max.chunk.size`
  (4 MB) it is closed (and forced to disk) and a new one started.
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

A map record store of a map with data persistence gets a mutation observer (core's hook for
Hazelcast's Enterprise record stores) that writes every put, update, removal, eviction, reset,
clear and destroy to the partition's log; the value carries the record's version, times, TTL and
max idle. A cache record store reports its record changes to a `CacheRecordObserver`, a hook
Hecaton adds to core; cache records are logged in their serialized form, and the cache's
configuration is kept, so caches created at runtime can be recreated. Data arriving by migration
or replication is written the same way, and a replica leaving the member clears it from the log;
destroys during shutdown are not logged. At start, each member reads its logs into the record
stores, on the partition threads, then builds the indexes of the partitions it owns.

## Cluster metadata and restart (F10.3, F10.4)

A member writes (atomically: temporary file, then rename) its UUID, the member list, the
partition table (replica UUIDs and versions) and the cluster state whenever they change, once its
start has completed. A restarted member reuses its UUID.

Recovery is a protocol of `RecoveryOperation`s driven by the master; every step can be repeated,
so a new master starts over:

1. Wait up to `validation-timeout-seconds` for the members that held replicas.
2. Collect each member's status (whether it has data, a stamp of its table and the sum of its
   partition versions) and check that they persisted the same table.
3. Ask every member to load its data for the table; a member deletes the logs of partitions it
   does not hold. Wait up to `data-load-timeout-seconds`.
4. Ask every member to install the table (`setInitialState`, with the members' current addresses)
   and complete its start; deferred post-join operations run then. Restore the cluster state.

Until step 4 no partition has an owner, so partition operations wait; partition assignment is
already held back by core while the start is not complete.

If members are missing or disagree, `FULL_RECOVERY_ONLY` fails. The partial policies (or a
partial-start request) keep the members with the most recent (highest version sum) or most common
table, remove the other members' replicas from it (the remaining replicas of a partition move up,
so a backup becomes owner), and, after completing, make the owners' replica versions newer than
the backups' so anti-entropy sends the backups again. Force start has every member delete its
data and metadata and complete its start as a new member. A member that finds the master already
started gives up its data the same way.

## Encryption at rest (F10, P1)

- `RecordCipher` encrypts each record's key and value separately with AES-GCM and a random 12-byte
  nonce (stored before the ciphertext, with the 16-byte tag after it). Record headers (lengths,
  sequence, CRC) stay in clear: the CRC covers the encrypted bytes, so recovery and crash
  consistency work as without encryption. Compaction copies the encrypted bytes as they are.
- The data key (AES, `key-size` bits) is created on the member's first start and kept in
  `<member directory>/encryption.key`, wrapped with AES-GCM under the master key (magic, format,
  nonce, ciphertext, written with `AtomicFiles`, so with a CRC).
- `MasterKeySource` reads the master keys, current first: `KeyStoreMasterKeySource` (AES secret key
  entries of a Java KeyStore) or `VaultMasterKeySource` (a Vault KV secret and its previous
  versions). At startup the member tries them in order; one other than the current key means the
  key was rotated, and the data key is wrapped again with the current one. With a
  `polling-interval`, a thread checks the store for a new current key.
- Hot backups copy `encryption.key` with the data.

## Backup (F10.5)

`PersistenceService#backup()` starts a `BackupOperation` on every member, which copies its
member file, metadata and structure ids, then, partition by partition on the partition threads,
the closed chunks (hard links where the file system allows) and the written part of the active
chunk, into `<backup-dir>/backup-<seq>/<member-dir>`. A backup is restored by using it as
`base-dir`.

## Testing

`PartitionLogTest` (store, torn writes, corruption, compaction, crash during compaction, a random
model), `PersistenceRestartTest` and `PersistenceRecoveryTest` (restarts, partial and force start,
rejoining members, caches, indexes, backups) and `CrashConsistencyTest` (members in their own
JVMs killed while writing).
