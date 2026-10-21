# Hecaton

A hundred hands, one mind. Hecaton is an open-source in-memory data grid that spreads your data
across many nodes and keeps it consistent.

Hecaton is a fork of Hazelcast Platform Community Edition 5.7.0, maintained as an independent
project under `uk.org.hecaton`. On top of everything Hazelcast CE does (distributed maps,
caches, queues, SQL, stream processing with Jet, clients in many languages), it provides
open-source implementations of features Hazelcast sells only in its Enterprise Edition:

- **TLS and security**: encrypted member, client and WAN connections; authentication with JAAS
  realms and client certificates; client permissions.
- **CP Subsystem and CPMap**: Raft-based linearizable locks, atomics, semaphores, latches and maps.
- **High-Density memory**: data off the Java heap, managed with the FFM API.
- **Persistence**: data and cluster metadata on disk, coordinated cluster restart, hot backups.
- **WAN replication**: asynchronous replication of maps and caches between clusters.
- **Client blue/green failover**, and no license key.

Official Hazelcast clients connect to Hecaton clusters. Cloud-provider integrations (AWS, Azure,
GCP) are not included; Kubernetes discovery is. Members run on Java 25; clients on Java 17+.

```sh
docker run -p 5701:5701 ghcr.io/l0ngsight/hecaton:<version>
```

- [Getting started](docs/getting-started.md)
- [Documentation](docs/README.md)
- [Migrating from Hazelcast](docs/migration.md) and [changes from Hazelcast](docs/changes-from-hazelcast.md)
- [Specification](docs/SPEC.md) and [implementation plan](docs/IMPLEMENTATION_PLAN.md)

Licensed under the [Apache License 2.0](LICENSE), except `hecaton-sql` and some Jet connectors,
which keep Hazelcast's [Community License](licenses/hazelcast-community-license.txt) (it does
not allow offering them as a competing managed service; spec §11.2). See [NOTICE](NOTICE),
[CONTRIBUTING](CONTRIBUTING.md) and [SECURITY](SECURITY.md).
