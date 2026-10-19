# Hecaton — Product & Technical Specification

| | |
|---|---|
| Status | Draft v0.1 |
| Upstream base | Hazelcast Platform Community Edition **v5.7.0** (tag `v5.7.0`, commit `60c31e3`) |
| Root package | `uk.org.hecaton` |
| Build JDK / member runtime | **Java 25** |
| Client runtime | **Java 17, 21, 25** |
| Companion document | [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) |

---

## 1. Summary

Hecaton is a fork of the latest Hazelcast Platform Community Edition (CE) that:

1. **Rebrands** the code base under the `uk.org.hecaton` root package. The rename is mechanical
   and reproducible, so migrating an application from Hazelcast to Hecaton is a
   find-and-replace of packages, a few class names, config file names and property prefixes
   (§5).
2. **Adds open-source implementations of the features Hazelcast sells only in Enterprise
   Edition (EE)**: TLS, security, persistence, WAN replication, CP Subsystem, High-Density
   memory, rolling upgrades, and others (§6). They are written independently through the
   extension points CE already has (§7).
3. **Stays mergeable with upstream**, so each new Hazelcast CE release can be rebased in
   with a scripted, mostly conflict-free process (§9).
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
- G5: Merging an upstream CE release (e.g. 5.8.0) takes days, not weeks.

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
| `uk.org.hecaton:hecaton` | upstream `hazelcast/` (renamed) | Java 17 | Apache 2.0 | Core API, member and client (same layout as upstream) |
| `uk.org.hecaton:hecaton-client` | slimmed copy of `hecaton` | Java 17 | Apache 2.0 | Client-only jar for Java 17/21/25 apps (§8.3) |
| `uk.org.hecaton:hecaton-ext-*` | new, Hecaton-owned | Java 25 | Apache 2.0 | The features in §6, one module per feature family (§7.3) |
| `uk.org.hecaton:hecaton-member` | new | Java 25 | Apache 2.0 | Convenience POM: `hecaton` plus every `hecaton-ext-*` module |
| `uk.org.hecaton:hecaton-sql` | upstream `hazelcast-sql` (renamed) | Java 17 | **HCL** | SQL engine, kept under its original license (§11.2) |
| `uk.org.hecaton:hecaton-spring`, `hecaton-spring-boot-autoconfiguration` | upstream (renamed) | Java 17 | Apache 2.0 | Spring integration |
| `uk.org.hecaton.jet:hecaton-jet-*` | upstream `extensions/*` (renamed) | Java 17 | Apache 2.0 / HCL per upstream header | Jet connectors |
| `hecaton-distribution` (zip/tar.gz), Docker `hecaton/hecaton` | — | — | mixed (NOTICE lists each) | Full member distribution |

### 3.2 Versioning

- Format: `<upstream major.minor.patch>-hc.<n>`, e.g. `5.7.0-hc.1`. Maven sorts unknown
  qualifiers after the release, so `5.7.0-hc.1` > `5.7.0`.
- **Cluster version and client-protocol version follow upstream** (`5.7`). Official
  Hazelcast clients negotiate with Hecaton exactly as they would with Hazelcast 5.7 (G3).
- `BuildInfo` reports the product name `Hecaton`, the Hecaton version, and the upstream
  version it is based on.

---

## 4. Architecture overview

```
                 ┌──────────────────────────────────────────────────────────────┐
 Applications ─▶ │ uk.org.hecaton:hecaton-client (Java 17+)  │ official HZ clients ─┐│
                 └──────────────────────────────────────────────────────────────┘│
                        client protocol 5.x (unchanged on the wire)               │
 ┌──────────────────────────────────────── Hecaton member (Java 25) ────────────▼┐
 │  uk.org.hecaton:hecaton  (renamed upstream CE, minimal patches)                   │
 │    NodeExtension SPI ◀── HecatonNodeExtension (hecaton-ext-core)               │
 │    ClientExtension SPI ◀── HecatonClientExtension                              │
 │                              │  FeatureProvider SPI (ServiceLoader)             │
 │      ┌──────────┬──────────┬─┴────────┬───────────┬──────────┬──────────┐       │
 │      │ security │   tls    │   cp     │persistence│   wan    │    hd    │ ...   │
 │      └──────────┴──────────┴──────────┴───────────┴──────────┴──────────┘       │
 └─────────────────────────────────────────────────────────────────────────────────┘
```

Principles:

1. **Upstream code changes only as far as the rename and a short list of tracked patches
   require** (`docs/upstream-patches.md`). Features live in Hecaton-owned modules.
2. **Use the existing SPI.** Every EE feature already has a hook in CE (`NodeExtension`,
   `ClientExtension`, `createService`, `SecurityContext`, `SSLEngineFactory`,
   `WanReplicationService`, `HotRestartService`, `CPSubsystem`, `UserCodeNamespaceService`,
   `AuditlogService`, `MemberSocketInterceptor`, ...). New hooks are added only when none
   fits, and each one is recorded as an upstream patch.
3. **Edition gates become feature checks.** `BuildInfo#isEnterprise()` call sites are
   replaced by `Features.isAvailable(Feature.X)`. A config that asks for a missing module
   fails with a precise message, e.g. "WAN replication requires `uk.org.hecaton:hecaton-ext-wan`
   on the classpath". `isEnterprise()` stays public for source compatibility and returns
   `true` when `hecaton-ext-core` is present.
4. **No licensing code.** `license-key` config is accepted and ignored with an INFO log, so
   existing EE configs still load.

---

## 5. Rebranding & migration contract

A versioned **rename table** (`tools/rebrand/rename-table.yaml`) drives every rename. The
same table generates:

- the fork itself (applied to upstream sources by `tools/rebrand`, §9), and
- the user migration tooling: an **OpenRewrite recipe** `uk.org.hecaton.migrate.FromHazelcast`
  and a standalone `hecaton-migrate` CLI for non-Java files.

Because both come from one table, "simple rename" is guaranteed by construction. Any
upstream symbol that would break the contract fails the rebrand CI job.

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
| Member protocol | No (NG1) | Join validation rejects non-Hecaton members with a clear message. |
| Serialized user data (IDS, Portable, Compact, Java serialization of user classes) | Yes | Factory/class IDs and Compact schemas are unchanged. |
| Java-serialized **`com.hazelcast.*` classes** in user data (rare: e.g. a stored `HazelcastJsonValue`) | No | Documented. The migration CLI can rewrite such stores offline. |
| Persistence / Hot Restart files | No (NG2) | |
| Cloud-provider discovery config (`aws`, `azure`, `gcp`, `eureka`, client `hazelcast-cloud`) | No (NG6) | Config load fails with a targeted error. Client-protocol codecs that carry these configs (e.g. dynamic WAN config) keep their wire layout; non-empty cloud sections are rejected. |
| REST endpoints | Renamed | Paths become `/hecaton/rest/...`, `/hecaton/health/...`. Requests to `/hazelcast/...` are served as `/hecaton/...` (same endpoint-group checks), so health checks, probes and existing tooling keep working. Memcache is unchanged. |
| SQL catalog name | Renamed | The catalog `hazelcast` becomes `hecaton`. Queries with unqualified or schema-qualified names (`mymap`, `public.mymap`) are unaffected; fully qualified names (`hazelcast.public.mymap`) must change. |
| Kubernetes label key `hazelcast.com/service-per-pod` | Yes | Kept, so existing manifests still work. |

### 5.5 Migration acceptance test

CI keeps a set of sample applications (embedded member, client, Spring Boot, Jet pipeline,
an EE-style config using TLS + security + WAN + persistence). For each one, CI runs the
OpenRewrite recipe and the CLI, builds, and runs the app against a Hecaton cluster. **The
only allowed diff is what the tooling produced.**

---

## 6. Feature scope (EE parity)

Priority: **P0** = needed for a production 1.0, **P1** = 1.x, **P2** = later.

| # | Feature | EE behaviour to match | Prio | CE hook(s) | Hecaton module |
|---|---|---|---|---|---|
| F1 | **TLS/SSL** (member, client, WAN, REST/Memcache; mutual TLS; cert hot-reload) | `SSLConfig` with `BasicSSLContextFactory` / OpenSSL factory, keystore/truststore, protocol and cipher selection | P0 | `NodeExtension#createSslEngineFactory`, `createChannelInitializerFn`; `ClientExtension` SSL path | `hecaton-ext-tls` |
| F2 | **Authentication** | `SecurityConfig` realms: simple, username/password, token, TLS client cert, LDAP, Kerberos, JAAS custom modules; member-to-member auth | P0 (LDAP/Kerberos P1) | `NodeExtension#getSecurityContext`, `SecurityService`, client `AuthenticationBaseMessageTask` | `hecaton-ext-security` |
| F3 | **Authorization** | Client permissions (`PermissionConfig`, all permission types incl. SQL, Jet, CP, vector), `SecurityInterceptor`, on-join permission policy | P0 | `InternalSecurityContext`, `SecurityInterceptor` | `hecaton-ext-security` |
| F4 | **Audit log** | `AuditlogConfig`, event types for security/cluster changes | P1 | `NodeExtension#getAuditlogService` | `hecaton-ext-security` |
| F5 | **Socket interceptor** | `MemberSocketInterceptor` on member and client | P1 | `NodeExtension#getSocketInterceptor`, `ClientExtension` | `hecaton-ext-security` |
| F6 | **Symmetric encryption** (deprecated upstream) | — | **Won't do.** Rejected with a message that points to F1 | — | — |
| F7 | **CP Subsystem** (Raft groups, `IAtomicLong`, `IAtomicReference`, `FencedLock`, `ISemaphore`, `ICountDownLatch`, CP sessions, membership management) | EE 5.5+ behaviour, `cp-member-count ≥ 3` | P0 | `NodeExtension#createCPSubsystem`, `CPSubsystemStubImpl` | `hecaton-ext-cp`, ported from Apache-2.0 `v5.4.0` |
| F8 | **CPMap** | `CPMap` API (put/set/get/remove/delete/compareAndSet/putIfAbsent/size limits) | P0 | `cp/internal/datastructures/cpmap` | `hecaton-ext-cp` (new code) |
| F9 | **CP persistence** | Raft log/snapshot persistence, restart recovery | P1 | `NodeExtension#getCPPersistenceService` | `hecaton-ext-cp` + `hecaton-ext-persistence` |
| F10 | **Persistence (Hot Restart)** for IMap, ICache, cluster metadata; cluster-wide backup; `ClusterState`-coordinated restart; partial-start policies; encryption at rest | `PersistenceConfig`, `DataPersistenceConfig`, `EncryptionAtRestConfig`, `PersistenceService` | P0 (encryption at rest P1) | `NodeExtension#getHotRestartService`/`getInternalHotRestartService`, `createService(MapService)`, `createService(ICacheService)`, cluster-state hooks | `hecaton-ext-persistence` |
| F11 | **High-Density memory** (`NATIVE` in-memory format) for IMap, ICache, Near Cache (member and client), HD indexes; `NativeMemoryConfig` (STANDARD and POOLED allocators, metadata space, persistent memory dir) | Same config and stats (`MemoryStats`) | P0 (member) / P1 (client near cache) | `ConfigValidator` NATIVE gates, `NodeExtension#getMemoryStats`, `createService(MapService/ICacheService)` | `hecaton-ext-hd` (Java 25 FFM, §8.4) |
| F12 | **Tiered Storage** (memory + disk tier for IMap) | `TieredStoreConfig`, `LocalDeviceConfig` | P2 | `ConfigValidator`, MapService | `hecaton-ext-tiered` (needs F11) |
| F13 | **WAN replication**: batch publisher, consumer, queue full behaviour, ack types, discovery-based endpoints, pause/stop/resume, dynamic add of WAN config, **WAN sync** (full + Merkle-tree delta), consistency check, WAN for IMap and ICache | `WanReplicationConfig`, `WanBatchPublisherConfig`, `WanSyncConfig`, REST/CLI operations | P0 (replication) / P1 (sync, consistency check) | `NodeExtension#createService(WanReplicationService)`, existing `merkletree` package | `hecaton-ext-wan` |
| F14 | **Rolling upgrades** (N-1 → N minor, cluster version auto-upgrade) | `supportedVersions = {N-1, N}`, `scheduleClusterVersionAutoUpgrade` | P1 (needs a second Hecaton minor release to be useful) | `NodeExtension#getSupportedVersions`, `isNodeVersionCompatibleWith`, `onClusterVersionChange` | `hecaton-ext-core` |
| F15 | **Client blue/green failover** | `ClientFailoverConfig`, `HazelcastFailoverClient` | P0 | `NodeExtension#isClientFailoverSupported` | `hecaton-ext-core` |
| F16 | **Dynamic configuration persistence**, **config reload/update** | `DynamicConfigurationConfig#persistenceEnabled`, config reload/update operations | P1 | `checkDynamicConfigurationPersistenceAllowed`, `createService(ClusterWideConfigurationService)` | `hecaton-ext-config` |
| F17 | **User Code Namespaces** | `UserCodeNamespacesConfig`, per-namespace classloaders for IMap, executors, Jet, ... | P1 | `NodeExtension#getNamespaceService` | `hecaton-ext-namespaces` |
| F18 | **Jet: lossless restart, isolated jobs, job upgrade / exported snapshots** | `JetConfig#losslessRestartEnabled`, `JobConfig` isolated-job flags, `Job#exportSnapshot` | P2 | `checkLosslessRestartAllowed`, `JetServiceBackend` | `hecaton-ext-jet` (needs F10) |
| F19 | **SQL catalog persistence** | `SqlConfig#catalogPersistenceEnabled` | P2 | `checkSqlCatalogPersistenceAllowed` | `hecaton-ext-config` (needs F10) |
| F20 | **Vector collection** (HNSW index, similarity metrics, filtering, backups) | `VectorCollectionConfig`, `VectorIndexConfig`, `com.hazelcast.vector` API | P2 | `vector` package, `VectorCollectionServiceUtil` | `hecaton-ext-vector` (on an Apache-2.0 ANN library such as jvector) |
| F21 | **License handling** | — | **Removed.** `license-key` is ignored, `getLicense()` returns a "community, unlimited" descriptor | `NodeExtension#getLicense` | `hecaton-ext-core` |
| F22 | **Management Center connectivity** | — | **Out of scope** (NG3) | — | — |

Every feature must:

- read the **unmodified (renamed) upstream config classes** and XML/YAML elements;
- expose the same **metrics names** (renamed per §5.2) and the same **REST/CLI
  operations** as documented for Hazelcast EE, where the operation exists in CE's REST/CLI
  surface;
- come with documentation in `docs/features/<feature>.md` that cites the public Hazelcast
  docs page whose behaviour it matches.

---

## 7. Extension design

### 7.1 `HecatonNodeExtension`

- Lives in `hecaton-ext-core` as `uk.org.hecaton.instance.impl.HecatonNodeExtension extends
  DefaultNodeExtension`.
- Registered by one upstream patch: `EXTENSION_PRIORITY_LIST` becomes
  `["uk.org.hecaton.instance.impl.HecatonNodeExtension", "uk.org.hecaton.instance.impl.DefaultNodeExtension"]`.
- Holds no feature logic. It discovers `FeatureProvider`s through `ServiceLoader` and
  delegates each SPI method to the provider that claims it, falling back to `super`.

### 7.2 `FeatureProvider` SPI

```java
package uk.org.hecaton.ext.spi;

public interface FeatureProvider {
    Set<Feature> features();                       // e.g. {TLS}
    default int order() { return 0; }
    void init(Node node, FeatureContext ctx);      // validate config, fail fast
    // optional capability interfaces a provider may implement:
    //   SslCapability, SecurityCapability, PersistenceCapability, CpCapability,
    //   WanCapability, NativeMemoryCapability, NamespaceCapability, AuditCapability, ...
}
```

- **Conflict rule:** at most one provider per capability. Startup fails if two claim the
  same one.
- **Client side:** `HecatonClientExtension extends DefaultClientExtension` is registered via
  `META-INF/services/uk.org.hecaton.client.impl.ClientExtension` (TLS, socket interceptor, HD
  near cache, failover).

### 7.3 Module map

```
hecaton-ext/
  hecaton-ext-spi          # FeatureProvider, Feature enum, capability interfaces (Java 17, used by both sides)
  hecaton-ext-core         # HecatonNodeExtension, HecatonClientExtension, failover, rolling upgrade, license no-op
  hecaton-ext-tls          # F1
  hecaton-ext-security     # F2–F5
  hecaton-ext-cp           # F7–F9
  hecaton-ext-persistence  # F10 (+ log-structured store shared by F9, F16, F18, F19)
  hecaton-ext-hd           # F11
  hecaton-ext-tiered       # F12
  hecaton-ext-wan          # F13
  hecaton-ext-config       # F16, F19
  hecaton-ext-namespaces   # F17
  hecaton-ext-jet          # F18
  hecaton-ext-vector       # F20
```

Client-relevant modules (`spi`, the client part of `core`, `tls`, the HD near-cache part of
`hd`) are split so that what ends up in a client classpath is Java 17 bytecode (§8.3).

### 7.4 Upstream patch budget

Every change to a renamed upstream file goes in `docs/upstream-patches.md` with its reason.
Target: **fewer than 40 patched upstream files**. Expected entries:

- extension priority list;
- `isEnterprise()` call sites → `Features` checks;
- `BuildInfo`/banner product name;
- join validation that rejects Hazelcast members;
- `legacy-names` compat lookup in config/property resolution;
- any SPI widening that §6 needs (e.g. a CPMap service hook).

---

## 8. Java platform strategy

### 8.1 Baselines

| Component | Compiled with | Bytecode (`--release`) | Runtime supported | Rationale |
|---|---|---|---|---|
| Build toolchain | JDK 25 | — | — | Single toolchain via Maven toolchains |
| `hecaton` (renamed upstream core) | JDK 25 | **17** | 17+ (client use), 25 (member use) | Keeps the upstream source baseline, so merges stay trivial and the shared client code runs on 17 |
| `hecaton-client` | JDK 25 | **17** | **17, 21, 25** | Requirement |
| `hecaton-ext-spi`, client parts of `hecaton-ext-core`/`-tls`/`-hd` | JDK 25 | **17** | 17+ | On the client classpath |
| Member-only `hecaton-ext-*` | JDK 25 | **25** | 25 | Free to use FFM, virtual threads, `ScopedValue`, structured concurrency, etc. |
| `hecaton-sql`, Spring, Jet connectors | JDK 25 | 17 | 17+ / 25 for members | Same as upstream |

### 8.2 Member runtime = Java 25

- `HecatonNodeExtension#beforeStart` fails unless `Runtime.version().feature() >= 25`, with
  the message "Hecaton members require Java 25+; the client library supports Java 17+".
- Member start scripts and the Docker image pass
  `--enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow` plus the
  upstream `--add-opens` set. JDK 24+ warns on `sun.misc.Unsafe` memory access
  (JEP 471/498), and upstream core still uses it in a few places. Hecaton-owned code
  **must not** use `sun.misc.Unsafe`.

### 8.3 Client library on 17 / 21 / 25

- Phase 1: `hecaton-client` is the `hecaton` jar (Java 17 bytecode) plus the client-side
  extension jars, published under the client coordinate. This is what upstream does today
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
- Upstream core code stays on Java 17 language level; it is **not** modernised in place
  (merge cost).

---

## 9. Upstream synchronisation model

```
upstream/main ──●──────────●────────────●  (one unmodified snapshot commit per upstream tag, no upstream history)
                v5.7.0      v5.8.0
                 │ tools/rebrand          │ tools/rebrand
rebrand/*   ─────●────────────────────────●  (generated only; never hand-edited)
                 │ merge                   │ merge (3-way, base = rebrand of previous tag)
main        ─────●──●──●──●──●──●──●───────●──…  (Hecaton patches + hecaton-ext modules)
```

- Upstream history is **not** imported (D8). `tools/upstream/import-snapshot.sh <tag>` adds
  the tree of each release tag as one commit on `upstream/main`, recording `Upstream-Tag` and
  `Upstream-Commit` trailers. The linear snapshot chain is enough for 3-way merges.
- `tools/rebrand` is a deterministic, token-aware transformer (identifiers, string literals,
  `package`/`import` lines) plus path moves and text rules for
  XML/YAML/XSD/properties/scripts/docs. It is driven by `rename-table.yaml`. The
  user-facing migration recipe (§5) is the type-aware OpenRewrite variant of the same
  table.
- It **never** modifies copyright/license headers, `LICENSE`, `NOTICE`, or third-party
  attribution text.
- A new upstream release = run rebrand on the new tag → commit to `rebrand/` → merge into
  `main`. Conflicts appear only where Hecaton patched upstream (§7.4).
- Security fixes that land on upstream `master` but in no CE release are fetched from the
  upstream repository as patches and cherry-picked through the same rebrand step
  (`tools/rebrand --commit <sha>`). No local upstream history is needed for this.

---

## 10. Quality requirements

| Area | Requirement |
|---|---|
| Upstream tests | The renamed upstream unit/integration suites pass on JDK 25. Tests that assert "requires Enterprise" are inverted to assert the feature works when the module is present and gives the Hecaton error when it is absent. |
| Feature tests | Each `hecaton-ext-*` module ships unit, integration (multi-member, `TestHazelcastFactory`-style) and failure tests (member kill, split brain, restart). |
| CP | The `v5.4.0` CP test suite is ported. Linearizability checking with a Jepsen-style harness (Knossos/Porcupine histories) on 3/5/7-member groups with partitions. |
| Persistence | Crash-consistency tests (kill -9 during writes, torn writes, fsync-off), cluster-wide restart, partial start, backup restore. |
| WAN | Two- and three-cluster topologies in Testcontainers, active/passive and active/active, sync after a partition. |
| Client compat | Official Hazelcast Java (5.5–5.7), Python, Go, .NET and Node.js clients run their smoke suites against a Hecaton cluster in CI. |
| Kubernetes | Nightly: a 3-member cluster on kind forms through Kubernetes discovery (DNS and API modes), survives a rolling restart, and a client inside the cluster connects. |
| Java matrix | Client suite on 17/21/25, member suite on 25. |
| Performance | JMH and `simulator`-style benchmarks vs. upstream CE 5.7 on-heap: no regression above 5% on IMap get/put. HD has its own targets (GC pause p99 < 10 ms with 100 GB native). |
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
- `hecaton` core and every `hecaton-ext-*` module **must not** depend on HCL modules
  (enforced by an ArchUnit/maven-enforcer rule), so the Apache-only member stays fully
  usable.
- Long-term option (not planned): a clean Apache-licensed SQL engine.

### 11.3 Clean-room rules for EE features

- Contributors **must not** read, decompile or run Hazelcast Enterprise source code, jars or
  Docker images for implementation purposes.
- Allowed sources: Hazelcast public documentation, public config schemas/XSDs, Apache-2.0
  upstream code (including historical tags such as `v5.4.0` for CP), academic papers (Raft,
  LSM/log-structured stores, Merkle trees, HNSW), and Apache/MIT third-party libraries.
- Each `hecaton-ext-*` PR template includes a provenance checkbox.

### 11.4 Trademarks

- "Hazelcast" is a trademark of Hazelcast, Inc. Hecaton does not use the name or logo for
  branding. Factual statements such as "a fork of Hazelcast CE" and "compatible with
  Hazelcast clients" are allowed in docs.
- The rename (§5) removes the mark from artifact names, packages, scripts and banners.

---

## 12. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Upstream refactors an SPI that Hecaton depends on | Merge pain | Keep the upstream patch budget small. Contract tests per SPI method catch breaks at rebrand-merge time. |
| No CE patch releases upstream | Unpatched CVEs | Watch upstream `master` and the advisories feed; `tools/rebrand --commit` cherry-pick flow; Hecaton patch releases (`-hc.N`). |
| Persistence/HD bugs cause data loss | High | P0 crash-consistency suite gates release; features ship as "preview" until it passes for 30 consecutive nightly runs. |
| Large scope (≈20 features) | Schedule | Phase plan with P0/P1/P2. 1.0 ships P0 only. |
| Unsafe removal in future JDKs breaks upstream core | Member runtime | Track JEP 471 follow-ups; Hecaton patches the few Unsafe call sites to VarHandle/FFM if upstream lags. |
| Upstream raises its baseline above 17 (e.g. 21) | Client 17 support | The client must then be built from a Hecaton-maintained compat branch, or Java 17 client support is dropped with notice. Re-evaluate at each upstream minor. |
| HCL scope grows upstream | More non-Apache code | Rebrand CI reports every HCL file. New HCL modules stay optional. |
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
| D8 | Upstream import as snapshots without history | Decided |
| D9 | Drop cloud-provider support (AWS, Azure, GCP, Eureka, Hazelcast Cloud); keep Kubernetes discovery (NG6) | Decided |

### 13.1 Domain and Maven namespace (D7)

**Decision:** the project domain is **`hecaton.org.uk`**, registered by the owner.

| Item | Value |
|---|---|
| Java root package | `uk.org.hecaton` (reverse-DNS of the domain) |
| Maven `groupId` | `uk.org.hecaton`. Maven Central verifies it with a DNS TXT record on `hecaton.org.uk`. |
| XML namespace | `https://hecaton.org.uk/schema/config` (and `/client-config`, `/spring`) |
| Unchanged | product name Hecaton, class names (`HecatonInstance`, ...), artifact IDs (`hecaton`, `hecaton-ext-*`), system properties (`hecaton.*`), env prefix `HC_`, scripts (`hc-*`) |

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
