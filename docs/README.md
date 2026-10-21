# Hecaton documentation

## Using Hecaton

- [Getting started](getting-started.md): run a member, connect a client, configure.
- [Running Hecaton in production](operations.md): sizing, discovery, Kubernetes, rolling
  restarts, monitoring.
- [Migrating from Hazelcast](migration.md): the migration tools, data copy, and Hazelcast
  Enterprise features in Hecaton.
- [Changes from Hazelcast 5.7](changes-from-hazelcast.md): what applications notice.

## Feature reference

Hecaton is Hazelcast CE 5.7 with these features added. For everything else (data structures,
SQL, Jet, clients, configuration), Hazelcast's 5.7 documentation applies with the names changed
as the [migration guide](migration.md) describes.

| Feature | Reference | Since |
|---|---|---|
| TLS/SSL, authentication, client permissions (F1–F3) | [security.md](security.md), [ee-class-map.md](ee-class-map.md) | 0.3.0 |
| CP Subsystem and CPMap (F7, F8) | [cp.md](cp.md) | 0.4.0; persistence (F9) 0.9.0 |
| High-Density memory (F11) | [hd.md](hd.md) | 0.5.0 |
| Persistence (F10) | [persistence.md](persistence.md) | 0.6.0 |
| WAN replication (F13) | [wan.md](wan.md) | 0.7.0; sync 0.9.0 |
| Audit log, socket interceptor (F4, F5) | [security.md](security.md) | 0.9.0 |
| Dynamic configuration persistence, reload (F16) | [dynamic-configuration.md](dynamic-configuration.md) | 0.9.0 |
| User Code Namespaces (F17) | [user-code-namespaces.md](user-code-namespaces.md) | 0.9.0 |
| Jet lossless restart, isolated jobs, job upgrades; SQL catalog persistence (F18, F19) | [jet.md](jet.md) | 0.10.0 |
| Client blue/green failover (F15) | [changes-from-hazelcast.md](changes-from-hazelcast.md) | 0.2.0 |

## The project

- [Specification](SPEC.md) and [implementation plan](IMPLEMENTATION_PLAN.md).
- [Releasing](releasing.md), [performance](performance.md), [security review](security-review.md).
- Designs of Hecaton's own components: [persistence](design/persistence.md); the other
  `design/` pages are inherited from Hazelcast.
- [Provenance](provenance.md) of code taken from other Hazelcast versions.
