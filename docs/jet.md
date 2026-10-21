# Jet: lossless restart, isolated jobs, job upgrades

Hecaton provides the Jet features that Hazelcast reserves for its Enterprise edition (F18), and
persists the SQL catalog (F19).

## Lossless restart

With `jet.lossless-restart-enabled` and [persistence](persistence.md) enabled, Jet's internal
maps (job records, execution records, snapshots and results: `__jet.*`) are persisted. After a
cluster restart the jobs that were running resume from their last snapshot:

```yaml
hecaton:
  persistence:
    enabled: true
    base-dir: /var/lib/hecaton/persistence
  jet:
    enabled: true
    lossless-restart-enabled: true
```

- Jobs need a processing guarantee (`EXACTLY_ONCE` or `AT_LEAST_ONCE`) to resume from a
  snapshot; the others restart from the beginning.
- The maps a job writes to are persisted only if their own configuration enables data
  persistence.
- Jobs start only once the cluster's start has completed, that is once persisted data is loaded.
- Lossless restart without persistence is refused at startup.

## SQL catalog persistence

With `sql.catalog-persistence-enabled` (and persistence), the SQL catalog (mappings, views,
types, data connections created with SQL: the `__sql.catalog` map) survives cluster restarts.
Without persistence the setting is refused at startup.

## Isolated jobs

A job submitted with a member selector (`JetService#newJobBuilder(...).withMemberSelector(...)`,
for example `JetMemberSelector.ALL_LITE_MEMBERS`) runs on the selected members only, for normal
and light jobs. If no member is selected, a normal job waits
until one is.

## Exported snapshots and job upgrades

`Job#exportSnapshot(name)` saves the state of a running job under a name, and
`Job#cancelAndExportSnapshot(name)` does so as the job's final snapshot and cancels it. A new job
(for example an upgraded version of the pipeline) starts from the exported state with
`JobConfig#setInitialSnapshotName(name)`. `JetService#getJobStateSnapshot(s)` lists the exported
snapshots; `JobStateSnapshot#destroy()` deletes one. The client and REST/Management Center job
operations are available too.

## Limitations

- A snapshot can be exported from a running job only: exporting the last snapshot of a suspended
  job, or the state of a job without a processing guarantee, is not supported.
- `Job#getName`-based lookup (`JetService#getJob(String)`) finds a job restored by a lossless
  restart only once the master has scanned the persisted job records; look jobs up by ID, or
  with `getJobs()`, right after a restart.

## Testing

`JetAndSqlPersistenceTest` (in `hecaton-persistence`: a streaming job resumes after a cluster
restart, the SQL catalog survives it), `IsolatedJobTest` (normal and light jobs run on the
selected member only) and `ExportedSnapshotTest` (export from a running job, cancel and export,
upgraded job started from the snapshot).
