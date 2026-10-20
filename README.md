# hecaton
A hundred hands, one mind. Hecaton is an open-source in-memory data grid that spreads your data across many nodes and keeps it consistent.

Hecaton is a fork of Hazelcast Platform Community Edition 5.7.0, maintained as an
independent project. It is renamed under `uk.org.hecaton` and adds open-source implementations of features that Hazelcast sells only
in Enterprise Edition: TLS, security, CP Subsystem, High-Density memory, persistence, WAN
replication, and more. Cloud-provider integrations (AWS, Azure, GCP) are not included; Kubernetes discovery is.

- [Specification](docs/SPEC.md)
- [Implementation plan](docs/IMPLEMENTATION_PLAN.md)
- [Migrating from Hazelcast](docs/migration.md) and [changes from Hazelcast](docs/changes-from-hazelcast.md)

Status: M1 (renamed code base) in progress on `feature/m1-rebranded-ce`.

Licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) and [CONTRIBUTING](CONTRIBUTING.md).
