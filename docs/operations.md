# Running Hecaton in production

## Members

- **Java 25.** Members refuse to start on older JVMs. Pass `--enable-native-access=ALL-UNNAMED`
  (the start scripts and the image do) for High-Density memory.
- **Memory.** On-heap data, its backups and the indexes live on the Java heap: size the heap for
  about three times the primary data with one backup, or move the data off the heap with
  [High-Density memory](hd.md), which keeps GC pauses short with large data sets.
- **Ports.** Members listen on 5701 (then 5702, ...) for members and clients, unless the
  [advanced network](https://github.com/l0ngsight/hecaton/blob/main/hecaton/src/main/resources/hecaton-full-example.yaml)
  separates them (member, client, WAN, REST and Memcache endpoints, each with its own TLS).
- **Discovery.** Multicast is on by default; in production use TCP/IP member lists, Kubernetes
  discovery or a Discovery SPI implementation. Cloud-provider discovery (AWS, Azure, GCP) is not
  part of Hecaton.
- **Cluster name.** Members and clients only talk to the cluster with their `cluster-name`:
  give every environment its own.
- **Security.** Turn on [TLS and authentication](security.md) for anything beyond a trusted
  network. Members send nothing anywhere (no usage reporting).

## Kubernetes

[`kubernetes/hecaton.yaml`](kubernetes/hecaton.yaml) runs three members as a StatefulSet behind
a headless service; apply [`kubernetes-rbac.yaml`](../kubernetes-rbac.yaml) first, which lets
members read the endpoints of their service:

```sh
kubectl apply -f kubernetes-rbac.yaml
kubectl apply -f docs/kubernetes/hecaton.yaml
```

Clients in the cluster connect to `hecaton.<namespace>.svc.cluster.local:5701`, or discover the
members through the Kubernetes API with the client's `kubernetes` discovery section. The nightly
`kubernetes-tests` job checks this setup on kind: the cluster forms in DNS and API modes, survives
a rolling restart, and a client pod connects.

For [persistence](persistence.md), give the StatefulSet a volume claim template mounted at the
`base-dir`, and keep the member names stable: a restarted pod finds its data in its own volume.
A Helm chart is not provided.

## Rolling restarts and shutdown

A member stopped with `hc-stop`, `SIGTERM` or `HecatonInstance#shutdown()` with the
`GRACEFUL` shutdown hook policy (`-Dhecaton.shutdownhook.policy=GRACEFUL`, as in the Kubernetes
example) first hands its partitions to the other members, so a rolling restart loses no data as
long as members are restarted one at a time and the cluster is safe in between
(`PartitionService#isClusterSafe`, `hc-cluster-admin -o get-state`). Give the pod a termination
grace period at least as long as `hecaton.graceful.shutdown.max.wait`.

All members of a cluster run the same Hecaton version; upgrading from one minor version to the
next without stopping the cluster (rolling upgrades, F14) is planned for 1.x. Until then upgrade
by restarting the cluster (with [persistence](persistence.md) to keep the data) or by moving
clients to a new cluster ([client failover](changes-from-hazelcast.md), `migrate data`).

To stop a whole cluster, `hc-cluster-admin -o shutdown` (or `Cluster#shutdown()`) changes the
cluster state to `PASSIVE` and stops every member, which persistence needs for a clean restart.

## Monitoring

- **Metrics:** every member collects metrics (maps, caches, queues, executors, partitions,
  networking, memory, CP, WAN, persistence). They are exposed as JMX MBeans
  (`-Dhecaton.jmx=true`) and, with `PROMETHEUS_PORT` set, by the bundled Prometheus JMX exporter
  (`config/jmx_agent_config.yaml`).
- **Health:** with the REST API enabled (`HC_NETWORK_RESTAPI_ENABLED=true`),
  `GET /hecaton/health/ready` answers 200 once the member has started and joined the cluster, and
  `/hecaton/health` reports the node and cluster state; `bin/hc-healthcheck` calls it.
- **Logs:** Log4j 2 (`config/log4j2.properties`, or `log4j2-json.properties` for JSON lines);
  `hecaton.logging.type` selects another framework.
- **Diagnostics:** `hecaton.diagnostics.enabled=true` writes periodic diagnostic reports (slow
  operations, invocations, thread dumps of hung threads) to `diagnostics-*.log`.

Management Center is a Hazelcast Enterprise product and is not part of Hecaton (spec NG3); the
management operations it uses are available through the Java API, REST and `hc-cluster-admin`.

## Backups and disaster recovery

- Partition backups (`backup-count`, `async-backup-count`) protect against member failures.
- [Persistence](persistence.md) protects against losing the whole cluster, and hot backups
  copy the persisted data while the cluster runs.
- [WAN replication](wan.md) keeps a copy in another data centre; clients move to it with
  [blue/green failover](changes-from-hazelcast.md).
