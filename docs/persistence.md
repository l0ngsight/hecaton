# Persistence

Persistence (F10, Hot Restart in Hazelcast) keeps the data of IMaps and ICaches, and the cluster
metadata, on disk, so a cluster that is shut down or fails as a whole restarts with its data. It
is provided by the `uk.org.hecaton:hecaton-persistence` module (Java 25); `hecaton-member` and
the distribution include it. The on-disk format is Hecaton's own: Hazelcast Hot Restart
directories cannot be read (spec NG2).

## Configuration

```yaml
hecaton:
  persistence:
    enabled: true
    base-dir: /data/hecaton/persistence
    backup-dir: /data/hecaton/backups      # for hot backups
    validation-timeout-seconds: 120
    data-load-timeout-seconds: 900
    cluster-data-recovery-policy: FULL_RECOVERY_ONLY   # or PARTIAL_RECOVERY_MOST_RECENT,
                                                       #    PARTIAL_RECOVERY_MOST_COMPLETE
    rebalance-delay-seconds: 0
  map:
    orders:
      data-persistence:
        enabled: true
        fsync: false
  cache:
    sessions:
      data-persistence:
        enabled: true
```

Only the structures with `data-persistence` enabled are persisted; the cluster metadata is
persisted whenever persistence is enabled. Caches created at runtime (`CacheManager#createCache`)
with data persistence are persisted with their configuration, and are recreated on restart.

Members can share a `base-dir`: each member works in a directory of its own below it, locked
while the member runs. A restarted member takes over the directory of an earlier member, with its
UUID and data.

### Durability

Every change is written to the member's log before the operation returns. Without `fsync` it is
in the operating system's cache then: it survives the failure of the member's process (`kill -9`),
not the failure of the machine. With `fsync: true` the log is forced to disk before the
operation returns; this costs a disk flush per change.

## Restarting a cluster

Shut the cluster down as a whole (`Cluster#shutdown()`, `hc-cluster-admin -o shutdown`, or the
REST call) and start its members again, in any order, with the same `base-dir`. The members may
come back with other addresses.

Recovery is driven by the master:

1. It waits for the members that held partition replicas before the restart, up to
   `validation-timeout-seconds`.
2. It checks that they persisted the same partition table.
3. It asks every member to load its data (on the partition threads), up to
   `data-load-timeout-seconds`.
4. It installs the persisted partition table, so every partition has the owner and backups it had
   before, and restores the cluster state (a cluster shut down as a whole comes back `ACTIVE`).

Until then partitions have no owner: partition operations wait, and members report
`isStartCompleted() == false`. The status is in Management Center (`ClusterHotRestartStatus`) and
in the member's log.

### When members are missing

With `FULL_RECOVERY_ONLY`, recovery fails when a member does not come back within the validation
timeout, or when the members persisted different tables; the members keep waiting. Then either
start the missing members and restart, or:

- **Partial start** (`PARTIAL_RECOVERY_MOST_RECENT`, `PARTIAL_RECOVERY_MOST_COMPLETE`, or asked
  for with the partial-start operation): the members that persisted the most recent (or the most
  common) partition table start with their data; the replicas of the others are left out of the
  table and their data is deleted. A backup takes the place of a missing owner, and the owners send
  their backups again. Partitions whose replicas were all on missing members are lost.
- **Force start** (the force-start operation): every member deletes its data and the cluster starts
  empty.

Both operations are available through `hc-cluster-admin -o force-start` / `partial-start`, REST
(`/hecaton/rest/management/cluster/forceStart`, `/partialStart`, in the `PERSISTENCE` endpoint
group) and Management Center.

A member that restarts on its own while the rest of the cluster is running gives up its data and
joins as a new member; the cluster's replicas are newer.

## Backups

With a `backup-dir`, `PersistenceService#backup()` (or REST `/hecaton/rest/management/cluster/backup`)
makes a hot backup: every member copies its data, partition by partition, to
`<backup-dir>/backup-<seq>/<member directory>` while the cluster keeps running. The closed log
files are hard links where the file system allows, so a backup is cheap. `getBackupTaskStatus()`
reports the progress; `interruptBackupTask()` stops it.

To restore a backup, start the members with `base-dir` set to `<backup-dir>/backup-<seq>`.

## On disk

Per partition, a log of chunk files; the active chunk is appended to and closed at 4 MB
(`hecaton.persistence.max.chunk.size`), and chunks that hold more garbage than live data are
compacted. The design is in [`design/persistence.md`](design/persistence.md).

## Limitations

- Encryption at rest (F10, P1) is planned for M9; members with it enabled refuse to start.
- CP Subsystem persistence (F9), dynamic configuration persistence (F16), SQL catalog persistence
  (F19) and Jet lossless restart (F18) come later; their settings are refused at startup.
- Maps configured at runtime (`Config#addMapConfig` on a running member) are not recreated:
  configure persisted maps statically.
- Not persisted: TTL changes with `IMap#setTtl`, expiration times extended by access in ICaches,
  last access times and hit counts.
- The keys' index of the logs is on the heap, also for `NATIVE` maps; compaction runs on the
  partition threads.
- After all members fail at once, a backup replica may lack the changes that were in flight at the
  failure (acknowledged changes are on both).
- `parallelism` is not used: data is loaded on the partition threads. `auto-remove-stale-data` is
  always on: stale data is deleted.
- Restarts managed by Kubernetes (`hecaton.persistence.auto.cluster.state`) are not supported yet.

## Testing

`hecaton-persistence`'s tests run on every CI build and in the nightly `persistence` group:
the store, cluster restarts (with other addresses, partial and force start, a member rejoining),
backups, and a crash-consistency suite that kills members (`kill -9`) in their own JVMs while one
of them writes, and checks that every acknowledged write survives.
