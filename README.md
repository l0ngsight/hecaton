# hecaton
A hundred hands, one mind. Hecaton is an open-source in-memory data grid that spreads your data across many nodes and keeps it consistent.

Hecaton is a fork of Hazelcast Platform Community Edition 5.7.0. It is renamed under
`org.hecatongrid` and adds open-source implementations of features that Hazelcast sells only
in Enterprise Edition: TLS, security, CP Subsystem, High-Density memory, persistence, WAN
replication, and more. Cloud-provider and Kubernetes integrations are not included.

- [Specification](docs/SPEC.md)
- [Implementation plan](docs/IMPLEMENTATION_PLAN.md)

Status: M0 (foundation). The Hazelcast CE 5.7.0 snapshot is on the `upstream/main` branch; the renamed code base arrives in M1.

Licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) and [CONTRIBUTING](CONTRIBUTING.md).
