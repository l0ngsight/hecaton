# Dynamic configuration

Data structure configurations (maps, caches, queues, topics, executors, WAN replication, ...) can
be added to a running cluster with `HecatonInstance#getConfig().addMapConfig(...)` and the other
`add...Config` methods, from members and clients; they reach every member, and members that join
later. Hecaton adds two Enterprise features on top (F16).

## Persistence

```yaml
hecaton:
  dynamic-configuration:
    persistence-enabled: true
    backup-dir: dynamic-configuration-backups   # default
    backup-count: 5                             # default
```

With persistence, every member writes the configurations added at runtime into its own
declarative configuration file (`hecaton.xml` or `hecaton.yaml`), so a restarted member has them
from the start. The member must have been started from a configuration file. Before each change
the file is copied to `backup-dir` (`<file name>.<timestamp>`), which keeps the last
`backup-count` copies; `backup-count: 0` keeps none.

The XML file gets the new section before `</hecaton>`; a YAML file is rewritten, which loses its
comments (they stay in the backups). Configurations that the file already has are not written
again. User code namespaces, vector collections and diagnostics added at runtime are not
persisted.

## Reload and update

A member can apply new data structure configurations to the cluster:

| Operation | API | REST (`CONFIG` endpoint group) |
|---|---|---|
| Reload the member's configuration file | `ConfigurationService#update()` | `POST /hecaton/rest/config/reload` |
| Apply a configuration (YAML or XML), asynchronously | `ConfigurationService#updateAsync(String)` | `POST /hecaton/rest/config/update` with the configuration as the third parameter |

Configurations the cluster does not have are added cluster-wide, as dynamic configurations.
Configurations that differ from the cluster's are ignored, since the configuration of a running
data structure cannot change; the result (and the REST response of a reload) lists the added and
the ignored ones. Other sections (network, properties, security, ...) are not reloaded: change
them by restarting the members.
