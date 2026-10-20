# Hecaton — Product & Technical Specification

| | |
|---|---|
| Status | Draft v0.2 |
| Forked from | Hazelcast Platform Community Edition **v5.7.0** (tag `v5.7.0`, commit `60c31e3`); one-time fork, maintained independently (D10) |
| Root package | `uk.org.hecaton` |
| Build JDK / member runtime | **Java 25** |
| Client runtime | **Java 17, 21, 25** |
| Companion document | [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) |

---

## 1. Summary

Hecaton is a fork of Hazelcast Platform Community Edition (CE) 5.7.0 that:

1. **Rebrands** the code base under the `uk.org.hecaton` root package. The rename is mechanical
   and reproducible, so migrating an application from Hazelcast to Hecaton is a
   find-and-replace of packages, a few class names, config file names and property prefixes
   (§5).
2. **Adds open-source implementations of the features Hazelcast sells only in Enterprise
   Edition (EE)**: TLS, security, persistence, WAN replication, CP Subsystem, High-Density
   memory, rolling upgrades, and others (§6). They are written independently through the
   extension points CE already has (§7).
3. **Is maintained as a project of its own.** The fork is taken once, from 5.7.0. Later
   Hazelcast releases are not merged in; Hecaton changes its code base freely, fixes its own
   bugs and security issues, and ports individual upstream fixes only when useful (§9).
4. **Builds on Java 25.** Members run on Java 25. The client library still targets Java 17
   and 21 (§8).

### 1.1 Goals

- G1: Swapping `com.hazelcast` for `uk.org.hecaton` (plus the rename table in §5) is the
  only change an existing Hazelcast application needs.
- G2: Every feature in §6 is available in the open-source distribution, with no license key,
  node limit or time bomb.
- G3: Hecaton members work with the **official Hazelcast clients** for Java, Python, Go,
  .NET, Node.js and C++ (5.x client protocol), so non-Java applications need no change
  beyond the cluster address.
- G4: The public config model (XML/YAML/programmatic) is Hazelcast's, renamed. A Hazelcast
  EE config file that uses features from §6 works after the §5 rename. The exception is
  cloud-provider configuration, which Hecaton removes (NG6).
- G5: Applications migrate from Hazelcast **5.7** (and older 5.x releases they can run
  on). Features Hazelcast adds after 5.7 are not tracked automatically; Hecaton decides
  feature by feature.

### 1.2 Non-goals

- NG1: **Mixed clusters.** Hazelcast and Hecaton members cannot be in the same cluster.
  Migration means standing up a Hecaton cluster, copying data with the client-based
  `hecaton-migrate data` tool (or a Jet job), and cutting clients over. A Hazelcast member
  trying to join is rejected with a clear error.
- NG2: **Reading Hazelcast EE on-disk data.** Hecaton's persistence format is its own. It
  cannot read Hazelcast Hot Restart or CP persistence directories.
- NG3: **Hazelcast Management Center.** Management Center is a separate proprietary product
  and compatibility is not a goal. Hecaton offers observability through metrics
  (JMX/Prometheus), REST and its CLI. A Hecaton console is a possible later project.
- NG4: **Using any Hazelcast EE code.** See the clean-room rules in §11.
- NG5: Wire compatibility for Hazelcast EE-only client features beyond those listed in §6
  (e.g. EE-only Management Center codecs).
- NG6: **Cloud-provider support (D9).** Hecaton removes the cloud-provider integrations
  that upstream CE ships:
  - member and client discovery for AWS, Azure, GCP and Eureka (`<aws>`, `<azure>`,
    `<gcp>`, `<eureka>` join/discovery config, also inside WAN publisher config);
  - client discovery for Hazelcast Cloud / Viridian (`ClientCloudConfig`,
    `HazelcastCloudDiscovery`);
  - the Jet connectors for AWS Kinesis and S3, and the S3/GCS/Azure file-system
    connectors (`hadoop-dist/files-*`);
  - upstream's cloud test infrastructure (Terraform).

  What stays: **Kubernetes** discovery (`<kubernetes>` config, DNS lookup and Kubernetes
  API modes, auto-detection when running in a pod, `kubernetes-rbac.yaml`) and the
  Kubernetes topology-intent tracker, which makes cluster-wide shutdown and persistence
  (F10) aware of StatefulSet scaling and rolling restarts. Also kept: TCP/IP and multicast
  join, the generic **Discovery SPI** (so a third party can add a Hecaton discovery
  plugin), partition groups, and the Docker image. A config that uses a removed element
  fails at load time with "Cloud-provider discovery is not supported in Hecaton; use
  tcp-ip, multicast, kubernetes or a Discovery SPI implementation".

---

## 2. Background: what upstream CE ships today

Facts taken from the `v5.7.0` source tree:

| Area | State in CE 5.7.0 |
|---|---|
| Licensing | Apache 2.0 by default. **Hazelcast Community License (HCL)** covers `hazelcast-sql` (~1,200 files) and the connector extensions `mongodb`, `hadoop`, `kinesis`, `kafka-connect`, `elasticsearch`, `mapstore`, `kafka`, `python`, `avro`, `grpc`, `s3`, `protobuf`, `csv` (~1,570 files in total). |
| Java | `jdk.version=17` (compile `--release 17`, enforcer requires JDK ≥ 17). |
| Core module | `hazelcast/` holds ~5,900 main-source Java files. Member and client live in **one jar**. |
| EE plug point (member) | `DefaultNodeContext.EXTENSION_PRIORITY_LIST` loads `com.hazelcast.instance.impl.EnterpriseNodeExtension` if it is on the classpath and falls back to `DefaultNodeExtension`. `NodeExtension` is the main SPI (security, SSL, sockets, persistence, CP, WAN, namespaces, TPC, client engine, versions, etc.). |
| EE plug point (client) | `HazelcastClientInstanceImpl#createClientInitializer` uses `ServiceLoader` for any `ClientExtension` other than `DefaultClientExtension`. |
| Edition detection | `BuildInfoProvider` looks for `com.hazelcast.instance.GeneratedEnterpriseBuildProperties`. About 15 classes branch on `BuildInfo#isEnterprise()`. |
| EE gates | `DefaultNodeExtension` and `ConfigValidator` reject at startup: persistence, security, symmetric encryption, audit log, Jet lossless restart, dynamic config persistence, SQL catalog persistence, CP Subsystem (`cpMemberCount > 0`), NATIVE in-memory format, Tiered Store, TLS, config reload/update, User Code Namespaces, WAN sync, Jet isolated jobs. |
| CP Subsystem | Only API and stubs remain (`CPSubsystemStubImpl`). The Raft engine and CP data structures were **removed after 5.4**. In `v5.4.0` they were Apache 2.0: ~375 main files under `com/hazelcast/cp`. `CPMap` never had an open implementation. |
| WAN | Config model, `WanReplicationServiceImpl` (custom publishers only, every management op throws `UnsupportedOperationException`) and Merkle tree code are present. The batch publisher, consumer and sync are missing. |
| Vector collection | Public API (`com.hazelcast.vector`) and config are present. There is no engine. |
| Release cadence | CE gets only minor releases (5.5.0, 5.6.0, 5.7.0). Patch releases are EE-only, so **Hecaton has to backport security fixes itself** (§12). |

---

## 3. Product definition

### 3.1 Artifacts

| Maven coordinate | Built from | Bytecode | License | Contents |
|---|---|---|---|---|
| `uk.org.hecaton:hecaton` | upstream `hazelcast/` (renamed) | Java 17 | Apache 2.0 | Core API, member and client, plus the features in §6 that run on Java 17 (§7.3) |
| `uk.org.hecaton:hecaton-client` | slimmed copy of `hecaton` | Java 17 | Apache 2.0 | Client-only jar for Java 17/21/25 apps (§8.3) |
| `uk.org.hecaton:hecaton-hd`, `hecaton-persistence`, `hecaton-tiered`, `hecaton-vector` | new, Hecaton-owned | Java 25 | Apache 2.0 | Member-only features that need Java 25 (FFM) or extra libraries (§7.3) |
| `uk.org.hecaton:hecaton-member` | new | Java 25 | Apache 2.0 | Convenience POM: `hecaton` plus every member module |
| `uk.org.hecaton:hecaton-sql` | upstream `hazelcast-sql` (renamed) | Java 17 | **HCL** | SQL engine, kept under its original license (§11.2) |
| `uk.org.hecaton:hecaton-spring`, `hecaton-spring-boot-autoconfiguration` | upstream (renamed) | Java 17 | Apache 2.0 | Spring integration |
| `uk.org.hecaton.jet:hecaton-jet-*` | upstream `extensions/*` (renamed) | Java 17 | Apache 2.0 / HCL per upstream header | Jet connectors |
| `hecaton-distribution` (zip/tar.gz), Docker `hecaton/hecaton` | — | — | mixed (NOTICE lists each) | Full member distribution |

### 3.2 Versioning

- **Product version:** Hecaton's own semantic version (D11). Pre-1.0 milestones are
  `0.x.y` (M1 exit: `0.1.0`); M8 releases `1.0.0`. It is independent of Hazelcast's
  version numbers.
- **Cluster version and client-protocol version** start at `5.7`, so official Hazelcast
  clients negotiate with Hecaton exactly as with Hazelcast 5.7 (G3). Hecaton raises the
  cluster version itself when a release changes the member protocol (rolling upgrades,
  F14); the client protocol stays 5.x-compatible.
- `BuildInfo` reports the product name `Hecaton`, the Hecaton version, and "based on
  Hazelcast 5.7.0".

---

## 4. Architecture overview

```
                 ┌──────────────────────────────────────────────────────────────┐
 Applications ─▶ │ uk.org.hecaton:hecaton-client (Java 17+)  │ official HZ clients ─┐│
                 └──────────────────────────────────────────────────────────────┘│
                        client protocol 5.x (unchanged on the wire)               │
 ┌──────────────────────────────────────── Hecaton member (Java 25) ────────────▼┐
 │  uk.org.hecaton:hecaton  (Java 17 bytecode: member + client)                     │
 │    TLS · security · CP · CPMap · WAN · failover · rolling upgrades · namespaces  │
 │    MemberModule SPI (ServiceLoader) ─────────────┐                               │
 │                                                  ▼                               │
 │      ┌──────────┬─────────────┬──────────┬──────────┐                            │
 │      │    hd    │ persistence │  tiered  │  vector  │   (Java 25 modules)        │
 │      └──────────┴─────────────┴──────────┴──────────┘                            │
 └─────────────────────────────────────────────────────────────────────────────────┘
```

Principles:

1. **The code base is Hecaton's.** Since no later upstream release is merged (D10),
   features are built where they fit best, including directly in core, and upstream code is
   refactored or deleted when that makes Hecaton simpler. `docs/changes-from-hazelcast.md`
   records the user-visible differences from Hazelcast 5.7, for migration.
2. **Features that run on Java 17 go into core.** TLS, security, CP, CPMap, WAN, failover,
   rolling upgrades, config persistence and User Code Namespaces are implemented in the
   `hecaton` module, behind the hooks CE already has (`NodeExtension`, `createService`,
   `SecurityContext`, `SSLEngineFactory`, `WanReplicationService`, `CPSubsystem`, ...).
   Only features that need Java 25 (FFM) or large extra dependencies are separate member
   modules, found through one small `MemberModule` SPI (§7).
3. **No edition gates.** The Enterprise checks (`BuildInfo#isEnterprise()`, the
   "requires Enterprise" rejections in `DefaultNodeExtension`/`ConfigValidator`) are
   removed. A config that needs a member module that is not on the classpath fails with a
   precise message, e.g. "NATIVE in-memory format requires `uk.org.hecaton:hecaton-hd` on
   the classpath". `isEnterprise()` stays as a deprecated method that returns `true`.
4. **No licensing code.** `license-key` config is accepted and ignored with an INFO log, so
   existing EE configs still load.

---

## 5. Rebranding & migration contract

A **rename table** (`tools/rebrand/rename-table.yaml`) produced the fork once
(`tools/rebrand` on the 5.7.0 sources, §9). Applications migrate with two tools that apply
the same rules:

- the **OpenRewrite recipe** `uk.org.hecaton.migrate.FromHazelcast`
  (`hecaton-migrate-rewrite`) for Java code and Maven/Gradle builds. It finds the
  `com.hazelcast` types, methods and constants an application uses and derives each
  Hecaton name with the fork's rules (package prefix swap, "Hazelcast" → "Hecaton" in
  identifiers), so no list of renamed API has to be maintained;
- the **`hecaton-migrate` CLI** for configuration, properties, environment, container,
  Kubernetes and script files.

The fork's catch-all `hazelcast` → `hecaton` text rule is not applied to applications: it
would also rename an application's own names (a map called `hazelcast.sessions`) in
configuration but not in code. The tools rename only names Hecaton reads: those in the
tables below, plus property keys and file names found as string literals in the Hecaton
sources (`known-names.txt`, kept current by a test). String literals in Java code get the
same rules as configuration files. Remaining mentions of Hazelcast are reported for review.

Because the fork and the tools share these rules, "simple rename" holds by construction for
the 5.7 API. Later Hecaton API changes must keep the contract: the migration acceptance
test (§5.5) guards it, and anything that cannot be renamed mechanically is listed in
`docs/changes-from-hazelcast.md`.

### 5.1 Java

| Kind | Hazelcast | Hecaton | Rule |
|---|---|---|---|
| Root package | `com.hazelcast.*` | `uk.org.hecaton.*` | prefix swap, sub-packages kept 1:1 |
| Type names | `Hazelcast`, `HazelcastInstance`, `HazelcastClient`, `HazelcastException`, `HazelcastJsonValue`, `HazelcastInstanceAware`, `HazelcastFailoverClient`, ... | `Hecaton`, `HecatonInstance`, `HecatonClient`, `HecatonException`, `HecatonJsonValue`, `HecatonInstanceAware`, `HecatonFailoverClient`, ... | `Hazelcast` → `Hecaton` within identifiers |
| Members | `newHazelcastInstance()`, `getHazelcastInstance()`, `setHazelcastInstance()`, `HazelcastClient.newHazelcastClient()` | `newHecatonInstance()`, `getHecatonInstance()`, `setHecatonInstance()`, `HecatonClient.newHecatonClient()` | same rule |
| Lower-case identifiers | `hazelcastInstance` | `hecatonInstance` | `hazelcast` → `hecaton` |
| EE class names users put in config (login modules, SSL factories, ...) | e.g. `com.hazelcast.security.loginimpl.LdapLoginModule` | `uk.org.hecaton.security.loginimpl.LdapLoginModule` | Hecaton ships a class at each renamed FQN listed in `docs/ee-class-map.md` |

Names that do **not** change: `Jet`, `IMap`, `ICache`, `CP*`, `Config` classes,
`DataSerializable`/`IdentifiedDataSerializable`, factory/class IDs, Compact type names.
These carry no brand and appear in user data.

### 5.2 Config, properties, environment, files

| Kind | Hazelcast | Hecaton |
|---|---|---|
| Member config files | `hazelcast.xml`, `hazelcast.yaml`, `hazelcast-default.xml` | `hecaton.xml`, `hecaton.yaml`, `hecaton-default.xml` |
| Client config files | `hazelcast-client.xml/yaml`, `hazelcast-client-failover.xml/yaml` | `hecaton-client.xml/yaml`, `hecaton-client-failover.xml/yaml` |
| XML root elements | `<hazelcast>`, `<hazelcast-client>`, `<hazelcast-client-failover>` | `<hecaton>`, `<hecaton-client>`, `<hecaton-client-failover>` |
| YAML root keys | `hazelcast:`, `hazelcast-client:` | `hecaton:`, `hecaton-client:` |
| XML namespace | `http://www.hazelcast.com/schema/config` (and `/client-config`, `/spring`) | `https://hecaton.org.uk/schema/config` (and `/client-config`, `/spring`) |
| XSD files | `hazelcast-config-5.7.xsd` | `hecaton-config-5.7.xsd` |
| System properties | `hazelcast.*` (≈400 keys, e.g. `hazelcast.config`, `hazelcast.logging.type`) | `hecaton.*` |
| Env config override | `HZ_` / `HZCLIENT_` prefixes | `HC_` / `HCCLIENT_` |
| JMX domain | `com.hazelcast` | `uk.org.hecaton` |
| System-property config override prefix | `-Dhz.<path>` (e.g. `hz.network.port.port`) | `-Dhc.<path>` |
| Thread names | `hz.<instance>.…` | `hc.<instance>.…` |
| CLI scripts | `hz`, `hz-start`, `hz-stop`, `hz-cli`, `hz-cluster-admin`, `hz-cluster-cp-admin`, `hz-healthcheck` | `hc`, `hc-start`, `hc-stop`, `hc-cli`, `hc-cluster-admin`, `hc-cluster-cp-admin`, `hc-healthcheck` |
| Maven | `com.hazelcast:hazelcast[-*]`, `com.hazelcast.jet:hazelcast-jet-*` | `uk.org.hecaton:hecaton[-*]`, `uk.org.hecaton.jet:hecaton-jet-*` |
| Spring Boot | `spring.hazelcast.config`, `HazelcastAutoConfiguration` | `spring.hecaton.config`, `HecatonAutoConfiguration` (shipped by `hecaton-spring-boot-autoconfiguration`; Spring Boot's own Hazelcast autoconfig does not trigger) |

### 5.3 Transitional compatibility (opt-in)

- `hecaton.compat.legacy-names=true` (default **off**) makes a member also read
  `hazelcast.*` system properties, `HZ_*` env vars and `hazelcast.xml/yaml` when no Hecaton
  equivalent exists, and logs a deprecation warning for each one. This covers ops tooling
  that cannot change together with the application. Java package aliases are **not**
  offered (split packages, duplicate classes).

### 5.4 Wire & data compatibility

| Surface | Compatible with Hazelcast 5.7? | Notes |
|---|---|---|
| Client protocol (binary codecs, auth, error codes) | **Yes** (G3) | Codec IDs and layouts stay as upstream. The server-side error class names sent in `ErrorHolder` are renamed. Clients map by error code, so this is harmless. |
| Member protocol | No (NG1) | Members open connections with `HCC` instead of Hazelcast's `HZC`. A Hecaton member closes a connection from a Hazelcast member and logs why, so the two cannot form one cluster. |
| Serialized user data (IDS, Portable, Compact, Java serialization of user classes) | Yes | Factory/class IDs and Compact schemas are unchanged. |
| Java-serialized **`com.hazelcast.*` classes** in user data (rare: e.g. a stored `HazelcastJsonValue`) | No | Documented. The migration CLI can rewrite such stores offline. |
| Persistence / Hot Restart files | No (NG2) | |
| Cloud-provider discovery config (`aws`, `azure`, `gcp`, `eureka`, client `hazelcast-cloud`) | No (NG6) | Config load fails with a targeted error. Client-protocol codecs that carry these configs (e.g. dynamic WAN config) keep their wire layout; non-empty cloud sections are rejected. |
| REST endpoints | Renamed | Paths become `/hecaton/rest/...`, `/hecaton/health/...`. Requests to `/hazelcast/...` are served as `/hecaton/...` (same endpoint-group checks), so health checks, probes and existing tooling keep working. Memcache is unchanged. |
| SQL catalog name | Renamed | The catalog `hazelcast` becomes `hecaton`. Queries with unqualified or schema-qualified names (`mymap`, `public.mymap`) are unaffected; fully qualified names (`hazelcast.public.mymap`) must change. |
| Kubernetes label key `hazelcast.com/service-per-pod` | Yes | Kept, so existing manifests still work. |

### 5.5 Migration acceptance test

CI keeps a set of sample applications under `migration-tests/` (embedded member, client,
Spring Boot, Jet pipeline, an EE-style config using TLS + security + WAN + persistence).
Each has the Hazelcast application (`before/`) and the reviewed result of migrating it
(`expected/`). For each one, CI runs the app against Hazelcast 5.7, runs the OpenRewrite
recipe and the CLI on a copy, requires the result to equal `expected/`, then builds and
runs the migrated app against Hecaton. **The only allowed diff is what the tooling
produced.**

---

## 6. Feature scope (EE parity)

Priority: **P0** = needed for a production 1.0, **P1** = 1.x, **P2** = later.

| # | Feature | EE behaviour to match | Prio | CE hook(s) | Where |
|---|---|---|---|---|---|
| F1 | **TLS/SSL** (member, client, WAN, REST/Memcache; mutual TLS; cert hot-reload) | `SSLConfig` with `BasicSSLContextFactory` / OpenSSL factory, keystore/truststore, protocol and cipher selection | P0 | `NodeExtension#createSslEngineFactory`, `createChannelInitializerFn`; `ClientExtension` SSL path | core |
| F2 | **Authentication** | `SecurityConfig` realms: simple, username/password, token, TLS client cert, LDAP, Kerberos, JAAS custom modules; member-to-member auth | P0 (LDAP/Kerberos P1) | `NodeExtension#getSecurityContext`, `SecurityService`, client `AuthenticationBaseMessageTask` | core |
| F3 | **Authorization** | Client permissions (`PermissionConfig`, all permission types incl. SQL, Jet, CP, vector), `SecurityInterceptor`, on-join permission policy | P0 | `InternalSecurityContext`, `SecurityInterceptor` | core |
| F4 | **Audit log** | `AuditlogConfig`, event types for security/cluster changes | P1 | `NodeExtension#getAuditlogService` | core |
| F5 | **Socket interceptor** | `MemberSocketInterceptor` on member and client | P1 | `NodeExtension#getSocketInterceptor`, `ClientExtension` | core |
| F6 | **Symmetric encryption** (deprecated upstream) | — | **Won't do.** Rejected with a message that points to F1 | — | — |
| F7 | **CP Subsystem** (Raft groups, `IAtomicLong`, `IAtomicReference`, `FencedLock`, `ISemaphore`, `ICountDownLatch`, CP sessions, membership management) | EE 5.5+ behaviour, `cp-member-count ≥ 3` | P0 | `NodeExtension#createCPSubsystem`, `CPSubsystemStubImpl` | core, ported from Apache-2.0 `v5.4.0` |
| F8 | **CPMap** | `CPMap` API (put/set/get/remove/delete/compareAndSet/putIfAbsent/size limits) | P0 | `cp/internal/datastructures/cpmap` | core (new code) |
| F9 | **CP persistence** | Raft log/snapshot persistence, restart recovery | P1 | `NodeExtension#getCPPersistenceService` | core + `hecaton-persistence` |
| F10 | **Persistence (Hot Restart)** for IMap, ICache, cluster metadata; cluster-wide backup; `ClusterState`-coordinated restart; partial-start policies; encryption at rest | `PersistenceConfig`, `DataPersistenceConfig`, `EncryptionAtRestConfig`, `PersistenceService` | P0 (encryption at rest P1) | `NodeExtension#getHotRestartService`/`getInternalHotRestartService`, `createService(MapService)`, `createService(ICacheService)`, cluster-state hooks | `hecaton-persistence` |
| F11 | **High-Density memory** (`NATIVE` in-memory format) for IMap, ICache, Near Cache (member and client), HD indexes; `NativeMemoryConfig` (STANDARD and POOLED allocators, metadata space, persistent memory dir) | Same config and stats (`MemoryStats`) | P0 (member) / P1 (client near cache) | `ConfigValidator` NATIVE gates, `NodeExtension#getMemoryStats`, `createService(MapService/ICacheService)` | `hecaton-hd` (Java 25 FFM, §8.4) |
| F12 | **Tiered Storage** (memory + disk tier for IMap) | `TieredStoreConfig`, `LocalDeviceConfig` | P2 | `ConfigValidator`, MapService | `hecaton-tiered` (needs F11) |
| F13 | **WAN replication**: batch publisher, consumer, queue full behaviour, ack types, discovery-based endpoints, pause/stop/resume, dynamic add of WAN config, **WAN sync** (full + Merkle-tree delta), consistency check, WAN for IMap and ICache | `WanReplicationConfig`, `WanBatchPublisherConfig`, `WanSyncConfig`, REST/CLI operations | P0 (replication) / P1 (sync, consistency check) | `NodeExtension#createService(WanReplicationService)`, existing `merkletree` package | core |
| F14 | **Rolling upgrades** (N-1 → N minor, cluster version auto-upgrade) | `supportedVersions = {N-1, N}`, `scheduleClusterVersionAutoUpgrade` | P1 (needs a second Hecaton minor release to be useful) | `NodeExtension#getSupportedVersions`, `isNodeVersionCompatibleWith`, `onClusterVersionChange` | core |
| F15 | **Client blue/green failover** | `ClientFailoverConfig`, `HazelcastFailoverClient` | P0 | `NodeExtension#isClientFailoverSupported` | core |
| F16 | **Dynamic configuration persistence**, **config reload/update** | `DynamicConfigurationConfig#persistenceEnabled`, config reload/update operations | P1 | `checkDynamicConfigurationPersistenceAllowed`, `createService(ClusterWideConfigurationService)` | core |
| F17 | **User Code Namespaces** | `UserCodeNamespacesConfig`, per-namespace classloaders for IMap, executors, Jet, ... | P1 | `NodeExtension#getNamespaceService` | core |
| F18 | **Jet: lossless restart, isolated jobs, job upgrade / exported snapshots** | `JetConfig#losslessRestartEnabled`, `JobConfig` isolated-job flags, `Job#exportSnapshot` | P2 | `checkLosslessRestartAllowed`, `JetServiceBackend` | core + `hecaton-persistence` |
| F19 | **SQL catalog persistence** | `SqlConfig#catalogPersistenceEnabled` | P2 | `checkSqlCatalogPersistenceAllowed` | core + `hecaton-persistence` |
| F20 | **Vector collection** (HNSW index, similarity metrics, filtering, backups) | `VectorCollectionConfig`, `VectorIndexConfig`, `com.hazelcast.vector` API | P2 | `vector` package, `VectorCollectionServiceUtil` | `hecaton-vector` (on an Apache-2.0 ANN library such as jvector) |
| F21 | **License handling** | — | **Removed.** `license-key` is ignored, `getLicense()` returns a "community, unlimited" descriptor | `NodeExtension#getLicense` | core |
| F22 | **Management Center connectivity** | — | **Out of scope** (NG3) | — | — |

Every feature must:

- read the **(renamed) Hazelcast 5.7 config classes** and XML/YAML elements, so EE
  configs carry over;
- expose the same **metrics names** (renamed per §5.2) and the same **REST/CLI
  operations** as documented for Hazelcast EE, where the operation exists in CE's REST/CLI
  surface;
- come with documentation in `docs/features/<feature>.md` that cites the public Hazelcast
  docs page whose behaviour it matches.

---

## 7. Extension design

### 7.1 Features in core

Features that run on Java 17 (§3.1) live in the `hecaton` module, in the packages where
Hazelcast EE users expect them (`uk.org.hecaton.security`, `uk.org.hecaton.cp`,
`uk.org.hecaton.wan`, ...). They plug into the existing hooks: `DefaultNodeExtension`
(renamed `HecatonNodeExtension` in M2) implements the security context, SSL engine factory,
CP subsystem, WAN service, socket interceptor, failover and version-compatibility methods
directly instead of rejecting them. There is no provider indirection.

The client side works the same way: `DefaultClientExtension` gains TLS, socket interceptor
and failover support in place.

### 7.2 `MemberModule` SPI

Java 25 features are separate jars so that core keeps Java 17 bytecode for the client
(§8.1):

```java
package uk.org.hecaton.instance.module;

public interface MemberModule {
    String name();                                  // "hd", "persistence", ...
    void init(Node node);                           // validate config, fail fast
    // optional capability interfaces:
    //   NativeMemoryCapability, PersistenceCapability, TieredStoreCapability,
    //   VectorCapability
}
```

- Found with `ServiceLoader` at member start. At most one module per capability; startup
  fails if two claim the same one.
- Core asks for a capability where it needs one (e.g. creating a NATIVE record store) and
  fails with the "requires `uk.org.hecaton:hecaton-<module>`" message when it is missing.

### 7.3 Module map

```
hecaton                 # core: member + client, Java 17; F1–F5, F7–F8, F13–F17, F21
hecaton-hd              # F11, Java 25 (FFM)
hecaton-persistence     # F9 storage, F10, F18/F19 storage; Java 25
hecaton-tiered          # F12 (needs hd + persistence)
hecaton-vector          # F20 (ANN library)
hecaton-member          # POM: hecaton + all member modules
```

F9 (CP persistence), F16 (dynamic config persistence), F18 and F19 keep their logic in
core and store data through `PersistenceCapability`.

## 8. Java platform strategy

### 8.1 Baselines

| Component | Compiled with | Bytecode (`--release`) | Runtime supported | Rationale |
|---|---|---|---|---|
| Build toolchain | JDK 25 | — | — | Single toolchain via Maven toolchains |
| `hecaton` (core: member + client) | JDK 25 | **17** | 17+ (client use), 25 (member use) | Member and client share one jar, and the client must run on 17 |
| `hecaton-client` | JDK 25 | **17** | **17, 21, 25** | Requirement |
| Member modules (`hecaton-hd`, `-persistence`, `-tiered`, `-vector`) | JDK 25 | **25** | 25 | Free to use FFM, virtual threads, `ScopedValue`, structured concurrency, etc. |
| `hecaton-sql`, Spring, Jet connectors | JDK 25 | 17 | 17+ / 25 for members | Same as upstream |

### 8.2 Member runtime = Java 25

- The member fails at start unless `Runtime.version().feature() >= 25`, with
  the message "Hecaton members require Java 25+; the client library supports Java 17+".
- Member start scripts and the Docker image pass
  `--enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow` plus the
  upstream `--add-opens` set. JDK 24+ warns on `sun.misc.Unsafe` memory access
  (JEP 471/498), and the inherited core still uses it in a few places. New code **must
  not** use `sun.misc.Unsafe`; the inherited call sites move to `VarHandle`/FFM over time.

### 8.3 Client library on 17 / 21 / 25

- Phase 1: `hecaton-client` is the `hecaton` jar (Java 17 bytecode), published under the
  client coordinate. This is what upstream does today
  (one jar), so it works immediately.
- Phase 2: a slimmed client jar built by reachability analysis from the client entry points
  (`HecatonClient`, `HecatonFailoverClient`, client config, codecs, serialization). It
  excludes member-only packages and is checked by an ArchUnit rule plus a smoke test.
- CI runs the client test suite on **Temurin 17, 21 and 25**, and member suites on 25 only.
- The enforcer's `enforceBytecodeVersion` is set per module (17 vs 25). A Java-17-targeted
  module that depends on a Java-25 module fails the build.

### 8.4 Java 25 features used by member modules

- **FFM API (`java.lang.foreign`)** for HD memory (F11): `Arena`, `MemorySegment`,
  `ValueLayout`. Replaces EE's Unsafe-based allocator; Tiered Store and persistence buffers
  build on it too.
- **Virtual threads** for blocking I/O in WAN publishers, persistence background tasks,
  LDAP/Kerberos calls.
- **Scoped values** for security and namespace context propagation in member-only code.
- Core stays on Java 17 language level while it is shared with the client. Once the slim
  client jar (§8.3 phase 2) exists, member-only packages can move to Java 25.

---

## 9. Relationship with upstream (D10)

Hecaton was created once from the Hazelcast CE `v5.7.0` tag and is maintained on its own
from then on:

- **No upstream merges.** Later Hazelcast releases are not imported. There is no patch
  budget and no requirement to keep upstream files close to their original form.
- **How the fork was made** (kept for provenance): the `v5.7.0` tree was imported as one
  snapshot commit without history (branch `upstream/main`, D8), renamed by `tools/rebrand`
  with `rename-table.yaml` (branch `rebrand/v5.7.0`), and merged into Hecaton. Both
  branches stay read-only as a record.
- **Individual upstream changes** (bug or security fixes in Hazelcast CE's Apache-2.0
  code) may be ported by hand when they apply, with a reference to the upstream commit.
  Hazelcast EE code stays off limits (§11.3).
- **Security:** Hecaton runs its own advisory process (`SECURITY.md`), watches Hazelcast's
  public CVEs and advisories for issues in inherited code, and ships patch releases.
- **Tools that only existed for syncing** (snapshot import, rebrand determinism job,
  upstream test suite in the nightly, `docs/upstream-patches.md` budget) are retired. The
  rename table stays as the record of the fork; the migration tools apply its rules (§5).

## 10. Quality requirements

| Area | Requirement |
|---|---|
| Inherited tests | The inherited unit/integration suites pass on JDK 25 (failures inherited from Hazelcast 5.7.0 are tracked in `docs/test-baseline/` until fixed). Tests that assert "requires Enterprise" are rewritten to assert that the feature works. |
| Feature tests | Each feature ships unit, integration (multi-member, `TestHazelcastFactory`-style) and failure tests (member kill, split brain, restart). |
| CP | The `v5.4.0` CP test suite is ported. Linearizability checking with a Jepsen-style harness (Knossos/Porcupine histories) on 3/5/7-member groups with partitions. |
| Persistence | Crash-consistency tests (kill -9 during writes, torn writes, fsync-off), cluster-wide restart, partial start, backup restore. |
| WAN | Two- and three-cluster topologies in Testcontainers, active/passive and active/active, sync after a partition. |
| Client compat | Official Hazelcast Java (5.5–5.7), Python, Go, .NET and Node.js clients run their smoke suites against a Hecaton cluster in CI. |
| Kubernetes | Nightly: a 3-member cluster on kind forms through Kubernetes discovery (DNS and API modes), survives a rolling restart, and a client inside the cluster connects. |
| Java matrix | Client suite on 17/21/25, member suite on 25. |
| Performance | JMH and `simulator`-style benchmarks vs. Hazelcast CE 5.7 on-heap: no regression above 5% on IMap get/put. HD has its own targets (GC pause p99 < 10 ms with 100 GB native). |
| Security | OWASP dependency check (upstream config reused), SpotBugs/FindSecBugs, CodeQL, TLS conformance tests (testssl.sh). |
| Docs | Every feature has a reference page and a migration note (Hazelcast EE config → Hecaton). |

---

## 11. Legal & licensing

### 11.1 Repository license

- Upstream code is Apache 2.0 and cannot be relicensed. The repository `LICENSE` was
  changed from MIT to **Apache 2.0** in M0 (D4).
- `NOTICE` keeps the full upstream NOTICE (Agrona, Koloboke, Akka, Calcite, Flink, ...) and
  adds: "This product includes software developed by Hazelcast, Inc., modified by the
  Hecaton project (package and product renaming, additional features)." This satisfies
  Apache 2.0 §4(b)/(d).
- Upstream copyright headers stay. New files carry the Hecaton header.

### 11.2 HCL-licensed upstream code

- `hazelcast-sql` and the HCL connectors are redistributed **unchanged in license** as
  separate artifacts (`hecaton-sql`, the matching `hecaton-jet-*`).
- The HCL "Excluded Purpose" forbids offering them as a competing managed (SaaS/PaaS)
  service. This applies to Hecaton downstream users too and is stated clearly in the README
  and in the distribution's `licenses/` folder.
- `hecaton` core and every member module **must not** depend on HCL modules
  (enforced by an ArchUnit/maven-enforcer rule), so the Apache-only member stays fully
  usable.
- Long-term option (not planned): a clean Apache-licensed SQL engine.

### 11.3 Clean-room rules for EE features

- Contributors **must not** read, decompile or run Hazelcast Enterprise source code, jars or
  Docker images for implementation purposes.
- Allowed sources: Hazelcast public documentation, public config schemas/XSDs, Apache-2.0
  upstream code (including historical tags such as `v5.4.0` for CP), academic papers (Raft,
  LSM/log-structured stores, Merkle trees, HNSW), and Apache/MIT third-party libraries.
- The PR template includes a provenance checkbox for EE-parity work.

### 11.4 Trademarks

- "Hazelcast" is a trademark of Hazelcast, Inc. Hecaton does not use the name or logo for
  branding. Factual statements such as "a fork of Hazelcast CE" and "compatible with
  Hazelcast clients" are allowed in docs.
- The rename (§5) removes the mark from artifact names, packages, scripts and banners.

---

## 12. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Hecaton owns every inherited bug and CVE (~6,000 core files) | Maintenance load | Own advisory process and patch releases; dependency updates automated (Renovate/Dependabot); port upstream CE fixes by hand where they apply (§9). |
| Hazelcast clients add protocol features after 5.7 | Newer official clients use features Hecaton lacks | Clients negotiate per feature; Hecaton documents the supported client-protocol level and tests official clients 5.5–5.7 plus newer ones as they appear (§10). |
| Persistence/HD bugs cause data loss | High | P0 crash-consistency suite gates release; features ship as "preview" until it passes for 30 consecutive nightly runs. |
| Large scope (≈20 features) | Schedule | Phase plan with P0/P1/P2. 1.0 ships P0 only. |
| Unsafe removal in future JDKs breaks inherited core code | Member runtime | Track JEP 471 follow-ups; move the remaining Unsafe call sites to VarHandle/FFM ahead of the removal. |
| Legal challenge to EE parity work | High | Strict clean-room process (§11.3), provenance records, no use of the Hazelcast name. |

---

## 13. Decisions

| # | Decision | Status |
|---|---|---|
| D1 | Upstream base `v5.7.0` | Decided |
| D2 | Root package `uk.org.hecaton` (reverse of the D7 domain), `Hazelcast*` → `Hecaton*`. Product name stays Hecaton. | Decided |
| D3 | Build on JDK 25, members on 25, client 17/21/25 | Decided |
| D4 | Repository license → Apache 2.0 (from MIT) | Decided (done in M0) |
| D5 | Keep HCL SQL/connectors as separate optional artifacts | Decided |
| D6 | Env prefix `HC_`/`HCCLIENT_`, CLI prefix `hc` | Decided |
| D7 | Domain `hecaton.org.uk`; package root, Maven `groupId` and XML namespace follow it (§13.1) | Decided; the owner is registering the domain |
| D8 | Upstream import as a snapshot without history | Decided (done; one import, `v5.7.0`) |
| D9 | Drop cloud-provider support (AWS, Azure, GCP, Eureka, Hazelcast Cloud); keep Kubernetes discovery (NG6). The config classes are deleted from the API. | Decided |
| D10 | One-time fork of 5.7.0; no later upstream releases are merged; Hecaton is maintained as its own project (§9) | Decided |
| D11 | Own semantic versioning (`0.x` before 1.0, `1.0.0` at M8); cluster version starts at 5.7 (§3.2) | Decided |

### 13.1 Domain and Maven namespace (D7)

**Decision:** the project domain is **`hecaton.org.uk`**, registered by the owner.

| Item | Value |
|---|---|
| Java root package | `uk.org.hecaton` (reverse-DNS of the domain) |
| Maven `groupId` | `uk.org.hecaton`. Maven Central verifies it with a DNS TXT record on `hecaton.org.uk`. |
| XML namespace | `https://hecaton.org.uk/schema/config` (and `/client-config`, `/spring`) |
| Unchanged | product name Hecaton, class names (`HecatonInstance`, ...), artifact IDs (`hecaton`, `hecaton-hd`, ...), system properties (`hecaton.*`), env prefix `HC_`, scripts (`hc-*`) |

**Availability and price** (Nominet RDAP and registrar price lists, 2026-10-18):

- `hecaton.org.uk`: free. About US$5.30/yr at Cloudflare (at cost), US$5.66/yr at Porkbun;
  renewal is the same price.
- `hecaton.uk`: also free, same price. Worth registering defensively; it is shorter for
  the website.
- `hecaton.co.uk`: taken.
- Earlier candidates: every `hecaton.<gTLD>` checked (.org, .com, .io, .dev, .net, .app,
  .cloud, .tech) is taken. Three were registered in 2026, so another project may be using
  the name "Hecaton". Check for trademark/name conflicts before 1.0. `hecatongrid.*` was
  considered and dropped.

The domain is needed for the Maven Central namespace check before the first publish (M8).
M1 can use the names without it.
