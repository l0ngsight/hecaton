# Hecaton — Implementation Plan

Companion to [`SPEC.md`](SPEC.md). Section references (§) point to the spec.

| | |
|---|---|
| Upstream base | Hazelcast CE `v5.7.0` |
| Target for 1.0 | All **P0** features from §6 on top of a renamed 5.7.x (or the latest upstream minor available at that point) |
| Effort unit | engineer-weeks (ew), rough order of magnitude |

---

## Milestones at a glance

| Milestone | Content | Exit criteria | Est. |
|---|---|---|---|
| **M0** Foundation | Repo license, upstream snapshot import, CI on JDK 25, contribution rules | CI green on an empty build; decisions D4–D6, D8 confirmed (D7 tracked separately) | 1–2 ew |
| **M1** Rebranded CE (`5.7.0-hc.0`) | `tools/rebrand`, renamed code base, cloud-native removal (D9), JDK 25 build, client 17/21/25 matrix, migration tooling | Upstream test suite passes (parity with upstream on JDK 25, minus removed cloud tests); official clients connect; migration acceptance test passes | 10–12 ew |
| **M2** Extension framework | `hecaton-ext-spi`, `hecaton-ext-core`, feature gates, Java 25 member check, client failover, license no-op | All EE gates converted; "missing module" errors tested | 3–4 ew |
| **M3** Security P0 | TLS (F1), authentication (F2, without LDAP/Kerberos), authorization (F3) | TLS conformance + security test suites green | 8–10 ew |
| **M4** CP Subsystem P0 | Raft port (F7), CPMap (F8) | Ported 5.4 CP test suite + linearizability harness green | 8–10 ew |
| **M5** HD memory P0 | FFM allocator, HD IMap/ICache, HD indexes (F11 member side) | HD suites green; GC/latency targets met | 14–18 ew |
| **M6** Persistence P0 | Log-structured store, IMap/ICache/metadata persistence, coordinated restart, backup (F10) | Crash-consistency suite green for 30 consecutive nightlies | 16–20 ew |
| **M7** WAN P0 | Batch publisher, consumer, management ops (F13 replication) | Multi-cluster Testcontainers suite green | 8–10 ew |
| **M8** Hecaton 1.0 | Hardening, perf, docs, security review, release | §10 quality gates met | 4–6 ew |
| **M9** 1.x (P1) | F4, F5, F9, F10 encryption at rest, F11 client near cache, F13 sync, F14, F16, F17, LDAP/Kerberos, slim client jar | per feature | 30–40 ew |
| **M10** 2.x (P2) | F12, F18, F19, F20 | per feature | 25–35 ew |

M3–M7 can run in parallel after M2. With about 5 engineers, 1.0 is realistic in roughly
8–10 months. The critical path is M1 → M2 → M5 → M6 (persistence builds on the HD/FFM
memory layer and on the shared store).

```
M0 ─▶ M1 ─▶ M2 ─┬─▶ M3 (security) ─────────────────────┐
                ├─▶ M4 (CP) ───────────────────────────┤
                ├─▶ M5 (HD) ─────▶ M6 (persistence) ───┼─▶ M8 (1.0) ─▶ M9 ─▶ M10
                └─▶ M7 (WAN) ──────────────────────────┘
   upstream sync (every upstream minor) runs continuously from M1 onward
```

---

## Branching model

Each milestone is developed on its own long-lived feature branch, cut from `main` once the
previous milestone it depends on has merged (or, for parallel milestones, from the M2
merge). PRs for individual tasks target the milestone branch. The milestone branch merges
into `main` once its exit criteria are met.

| Milestone | Feature branch |
|---|---|
| M0 Foundation | `feature/m0-foundation` |
| M1 Rebranded CE | `feature/m1-rebranded-ce` |
| M2 Extension framework | `feature/m2-extension-framework` |
| M3 Security P0 | `feature/m3-security` |
| M4 CP Subsystem P0 | `feature/m4-cp-subsystem` |
| M5 HD memory P0 | `feature/m5-hd-memory` |
| M6 Persistence P0 | `feature/m6-persistence` |
| M7 WAN P0 | `feature/m7-wan-replication` |
| M8 Hecaton 1.0 | `feature/m8-release-1.0` |
| M9 1.x (P1) | `feature/m9-p1-features` |
| M10 2.x (P2) | `feature/m10-p2-features` |

Upstream sync work uses `sync/vX.Y.Z`; generated rebrand output lives on `rebrand/vX.Y.Z`
(§9 of the spec).

---

## Phase 0 — Foundation (M0)

1. **Decisions** (§13): D4 Apache 2.0 relicense, D5 HCL modules as optional artifacts,
   D6 `HC_`/`hc` prefixes, D8 snapshot import without history are decided. D7 (domain,
   XML namespace, Maven `groupId`) is open pending a domain purchase (§13.1) and blocks only
   the final values in `rename-table.yaml` (M1).
2. **License files:** replace `LICENSE` with Apache 2.0, add `NOTICE` (upstream NOTICE plus
   the Hecaton modification statement, §11.1), add `licenses/` with the Apache and HCL texts.
3. **Upstream snapshot import** (no history, D8):
   `tools/upstream/import-snapshot.sh v5.7.0` creates the orphan branch `upstream/main` with
   one commit holding the `v5.7.0` tree (`Upstream-Tag`/`Upstream-Commit`/
   `Upstream-Excludes-SHA256` trailers). Files are byte-identical to upstream. Paths in
   `tools/upstream/excluded-paths.txt` are left out: upstream's `.github/` (its CI would
   run here) and the self-contained cloud-native code (D9), 229 files for v5.7.0. Later
   releases are added the same way, one commit per tag.
4. **CI skeleton** (GitHub Actions, `.github/workflows/ci.yml`; the logic lives in
   `tools/ci/*.sh` so later milestones only change scripts):
   - `hygiene`: Apache `LICENSE`/`NOTICE` present, no Hazelcast Enterprise artifacts or
     repositories in build files, no `sun.misc.Unsafe` in Hecaton-owned code.
   - `build`: JDK 25 (Temurin). Skips the Maven build until M1 adds `pom.xml`; M1 also turns
     on `setup-java`'s Maven cache, which fails when no `pom.xml` exists.
   - `client`: JDK 17/21/25 matrix (JDK 25 also installed for the toolchain). M1 adds the
     `client-tests` Maven profile it calls.
   - Added later: `nightly.yml` (full suites, crash-consistency, multi-cluster, compat
     clients) in M1, `rebrand-check.yml` (determinism, HCL file report) with `tools/rebrand`.
5. **Contribution rules:** `CONTRIBUTING.md` with the clean-room policy (§11.3), DCO
   sign-off, PR template with a provenance checkbox, `CODEOWNERS`, `SECURITY.md`.

---

## Phase 1 — Rebrand tool and renamed baseline (M1)

### 1.1 `tools/rebrand`

A standalone Java 25 CLI in `tools/rebrand/`. Deterministic and idempotent:
`rebrand(upstreamTree) → hecatonTree`.

| Component | Work |
|---|---|
| `rename-table.yaml` | Single source of truth for §5: package prefix, identifier rules (`Hazelcast`→`Hecaton`, `hazelcast`→`hecaton`), file/path renames, property/env prefixes, XML namespaces, Maven coordinates, script names, metric/JMX/thread prefixes. |
| **Keep-list** (must *not* be renamed; wire- or data-visible) | Distributed-object service names (`hz:impl:mapService`, `hz:impl:*`, `hz:core:*`, `hz:raft:*` …) that official clients send in protocol messages; protocol magic bytes (`HZC`, `CP2`); codec IDs; the server version string format; factory/class IDs; Compact type names; internal object names (`__sql.catalog`, `__jet.*`); copyright headers, `LICENSE`, `NOTICE`, third-party attributions. The keep-list is tested: a fixture of official-client traffic is replayed against the renamed member. |
| Java transformer | Lexical, token-aware rewriting (identifiers, string literals, comments, `import`/`package` lines) driven by the table. Type attribution is not needed because all rules are prefix/substring based. Also covers `META-INF/services/*` file names and contents, reflection strings (e.g. `EXTENSION_PRIORITY_LIST`), `Automatic-Module-Name` (`com.hazelcast.core` → `org.hecatongrid.core`). |
| Path mover | `com/hazelcast/**` → `org/hecaton/**` in every source set; `hazelcast-*` module dirs and artifact IDs → `hecaton-*`; `hazelcast-config-*.xsd` → `hecaton-config-*.xsd`; `hz-*` scripts → `hc-*`. |
| Resource rules | XML/YAML/XSD/properties/JSON/shell/bat/Dockerfile/Helm text rules; `<hazelcast>` root element; namespace URIs; `HZ_` env prefix; `hazelcast.*` property keys. |
| Codec generator | The client-protocol codecs are checked in, so they are renamed like other sources. Fork the generator templates (`hazelcast-client-protocol`) only to change package lines. Codec layouts must not change. |
| Verification | (a) The output compiles. (b) Running twice gives no diff (idempotence). (c) Scanning the output finds no `com.hazelcast`/`Hazelcast` identifiers outside the keep-list. (d) A report lists every remaining "hazelcast" occurrence with its justification. |

Est.: 3–4 ew (including edge-case triage on ~5,900 core files plus modules/tests/resources).

### 1.2 Renamed baseline on JDK 25

1. Run rebrand on `v5.7.0` and commit the result to `rebrand/v5.7.0` (generated, one commit
   on top of the `upstream/main` snapshot).
2. Merge `rebrand/v5.7.0` into `main` (unrelated-history merge the first time; later
   merges are normal 3-way).
   - **Binary test fixtures:** upstream ships 10 test jars (e.g.
     `client/console/testjob-*.jar`, `jet/core/DataSerializableSuppliers.jar`) and
     serialization-compatibility binaries that contain compiled `com.hazelcast` class names.
     Rebrand cannot rewrite them; rebuild them from source (or regenerate the binaries) as
     part of this step and list them in `docs/upstream-patches.md`.
3. **Build on JDK 25:**
   - Add a Maven toolchains config; set `jdk.version`-style properties per module:
     `hecaton.release.client=17`, `hecaton.release.member=25`.
   - Keep `--release 17` for every renamed upstream module (§8.1).
   - Upgrade build plugins and test libraries only where JDK 25 needs it (Mockito/ByteBuddy,
     SpotBugs, Checkstyle, JaCoCo, Surefire `argLine` with
     `--sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED`). Each one is
     recorded as an upstream patch.
   - Per-module `enforceBytecodeVersion` (17 or 25) and a rule that forbids 17-targeted
     modules from depending on 25-targeted ones.
4. **Baseline comparison:** run the *upstream* `v5.7.0` suite on JDK 25 and the *renamed*
   suite on JDK 25. The renamed suite must match the upstream pass/fail set exactly. Known
   upstream failures go in `docs/known-upstream-failures.md`.
5. **Product identity:** `BuildInfo`, startup banner, `hecaton-default.xml`, distribution
   (`hc-*` scripts), Docker image `hecaton/hecaton:5.7.0-hc.0` (Temurin 25 JRE base).

Est.: 3 ew.

### 1.3 Client library 17/21/25 (phase 1 of §8.3)

- Publish `org.hecatongrid:hecaton-client`, which at this stage re-publishes the Java 17 core
  jar under the client coordinate.
- Add a client-test profile (the upstream `client` test packages plus new smoke tests) to
  `client-matrix.yml` on Temurin 17, 21 and 25.

Est.: 0.5 ew.

### 1.4 Compatibility smoke tests

- Testcontainers job: a Hecaton member image, with official **Hazelcast** clients for Java
  (5.5, 5.6, 5.7), Python, Go, .NET and Node.js running basic IMap/queue/topic/SQL/Compact
  scenarios.
- Negative test: a Hazelcast 5.7 member trying to join a Hecaton cluster is rejected with
  the documented message (needs the join-validation patch, which can land early).

Est.: 1.5 ew.

### 1.5 Migration tooling (§5)

| Deliverable | Work |
|---|---|
| `hecaton-migrate-rewrite` | OpenRewrite recipe `org.hecatongrid.migrate.FromHazelcast`, generated from `rename-table.yaml`: `ChangePackage`, `ChangeType`, `ChangeMethodName`, Maven/Gradle dependency changes, Spring property keys. |
| `hecaton-migrate` CLI | Non-Java files: config XML/YAML, `.properties`, env files, Dockerfiles, Helm/K8s manifests, shell scripts. Dry-run diff mode. |
| `hecaton-migrate data` | Client-based copier (Hazelcast cluster → Hecaton cluster) for IMap/ICache/MultiMap/ReplicatedMap, using official Hazelcast client + Hecaton client in one JVM. |
| Acceptance | The §5.5 sample apps under `migration-tests/`. CI fails if anything beyond tool output had to change. |

Est.: 2.5 ew.

### 1.6 Remove cloud-native support (D9, spec NG6)

`tools/upstream/excluded-paths.txt` already leaves out the self-contained plugin and
connector directories. M1 removes what the rest of the code base references, as tracked
upstream patches:

| Area | Work |
|---|---|
| Config model | Remove `AwsConfig`, `AzureConfig`, `GcpConfig`, `KubernetesConfig`, `EurekaConfig`, `ClientCloudConfig` and their accessors on `JoinConfig`, `ClientNetworkConfig`, `WanBatchPublisherConfig`; trim `AliasedDiscoveryConfig`/`AliasedDiscoveryConfigUtils` to nothing or delete them. |
| Config loading | XML/YAML parsers, config generators, XSD/JSON schemas, `HC_` env overrides: drop the elements. The parsers keep recognising the element names only to throw the targeted NG6 error. |
| Client protocol | Keep the wire layout of `AwsConfigCodec` & co. (G3). Decode and reject non-empty cloud sections in dynamic-config messages. |
| Member/client runtime | Remove `KubernetesTopologyIntentTracker` usage from `Node` (keep the no-op tracker); remove `HazelcastCloudDiscovery`/`ViridianAddressProvider` from the client; clean `META-INF/services/...DiscoveryStrategyFactory`; fix references in `DefaultReflectiveAttributeLookupPolicy` and `DefaultSerializationServiceBuilder`. |
| Build & distribution | Remove the excluded modules from `extensions/pom.xml`, `hadoop-dist/pom.xml`, `distribution`; check `hadoop-all` for bundled cloud file systems. |
| Tests & docs | Delete tests of removed features; add tests for the NG6 config error; note the removal in the migration guide. |

Est.: 1.5–2 ew.

### 1.7 Upstream top-level files

Upstream's `README.md`, `LICENSE`, `NOTICE`, `CONTRIBUTING.md`, `SECURITY.md` and
`codecov.yml` collide with Hecaton's. Rebrand moves the upstream versions to
`docs/upstream/` (the Hecaton `NOTICE` already embeds the upstream NOTICE), so merges never
conflict on them.

**M1 exit:** tag `v5.7.0-hc.0` (CE parity minus cloud-native support, renamed, Java 25 build).

---

## Phase 2 — Extension framework (M2)

1. **`hecaton-ext-spi`** (Java 17): `Feature` enum (one constant per F-number),
   `FeatureProvider`, capability interfaces, `Features.isAvailable(Feature)`,
   `MissingFeatureException` with module hint.
2. **`hecaton-ext-core`:**
   - `HecatonNodeExtension extends DefaultNodeExtension` delegates through providers
     (§7.1–7.2). Upstream patch: `EXTENSION_PRIORITY_LIST`.
   - `HecatonClientExtension extends DefaultClientExtension`, registered via
     `META-INF/services`.
   - Java 25 member runtime check (§8.2).
   - F21: license no-op (`license-key` accepted and ignored; `getLicense()` descriptor).
   - F15: `isClientFailoverSupported() → true`, plus failover tests with two clusters.
3. **Gate conversion** (upstream patches): replace every `BuildInfo#isEnterprise()` check
   in `DefaultNodeExtension`, `ConfigValidator`, `Node`, `ClientEndpointImpl`,
   `ChannelInitializerFunction`, `DefaultClientExtension`, etc. with `Features` checks;
   `isEnterprise()` returns `true` when `hecaton-ext-core` is present. Invert the upstream
   tests that assert "requires Enterprise".
4. **`docs/upstream-patches.md`** started, with a CI check that every non-generated change
   to a file under a renamed upstream module has an entry.
5. **`hecaton-member` BOM/POM** that pulls in every ext module.

Est.: 3–4 ew.

---

## Phase 3 — Security P0 (M3): `hecaton-ext-tls`, `hecaton-ext-security`

| Step | Work | Est. |
|---|---|---|
| F1.1 | `SSLEngineFactory` implementations: JDK (`BasicSSLContextFactory`-compatible properties: keystore/truststore type/password/path, protocol, ciphers, `mutualAuthentication` REQUIRED/OPTIONAL) and optional OpenSSL via netty-tcnative (Apache 2.0). | 2 ew |
| F1.2 | TLS channel handlers in `createChannelInitializerFn` for MEMBER, CLIENT, WAN, REST, MEMCACHE endpoint qualifiers (advanced networking). Handshake timeout, ALPN not required. | 1.5 ew |
| F1.3 | Client-side TLS (`HecatonClientExtension`), Java 17 compatible. | 1 ew |
| F1.4 | Keystore hot-reload (`keyStoreReload`/file-watch), cert expiry metrics. | 0.5 ew |
| F2.1 | `SecurityContext`/`SecurityService`: realms, JAAS login, `Credentials` types (username/password, token, cert), member-to-member authentication, client authentication in `AuthenticationBaseMessageTask`. | 2.5 ew |
| F2.2 | Built-in login modules at the renamed EE class names (`docs/ee-class-map.md`): simple, username/password config realm, TLS cert, token. | 1 ew |
| F3.1 | Permission evaluation for every `PermissionConfig.PermissionType` (map, cache, queue, topic, lock, atomic, executor, SQL, Jet job/connector, CP, user code namespace, vector, config, transaction, all); wildcards and principal matching; `SecurityInterceptor` chain; on-join permission policy (RECEIVE/SEND/NONE). | 2.5 ew |
| Tests | Port of the public permission semantics into tests; TLS conformance (`testssl.sh`) in nightly; fuzzed auth messages. | incl. |

---

## Phase 4 — CP Subsystem P0 (M4): `hecaton-ext-cp`

| Step | Work | Est. |
|---|---|---|
| F7.1 | Import `com/hazelcast/cp/**` (main and test) from `v5.4.0` (Apache 2.0, ~375 main files) into `hecaton-ext-cp` through `tools/rebrand --tree`. Keep the original headers; add provenance to `NOTICE`/`docs/provenance.md`. | 0.5 ew |
| F7.2 | Diff the CP public/SPI surface between 5.4.0 and 5.7.0 (`CPSubsystem`, `CPSubsystemManagementService`, `CPSubsystemConfig`, CP group/session APIs, metrics, client codecs) and adapt the ported engine to it. Replace `CPSubsystemStubImpl` via `createCPSubsystem`. | 3 ew |
| F7.3 | Register CP client message tasks (codecs exist in core), CP membership REST/CLI (`hc-cluster-cp-admin`). | 1 ew |
| F8 | `CPMap` new implementation: Raft-replicated KV state machine with the documented ops and size limits, snapshotting, client proxy, config (`CPMapConfig`). | 2.5 ew |
| Tests | Ported 5.4 CP tests (~114 files) + CPMap tests + linearizability harness (Porcupine-style checker over recorded histories, nemesis: partitions, pauses, kills). | 2 ew |

---

## Phase 5 — High-Density memory P0 (M5): `hecaton-ext-hd`

| Step | Work | Est. |
|---|---|---|
| F11.1 | **FFM memory layer** (Java 25): `MemorySegment`-based `MemoryAllocator` with STANDARD (malloc-like via `Arena`) and POOLED (power-of-two buddy allocator with page/min-block sizes per `NativeMemoryConfig`), metadata space, thread-local allocation caches, native OOM handling, `MemoryStats`. | 4 ew |
| F11.2 | Off-heap data structures: open-addressing hash map with off-heap keys/values, slab-allocated records, off-heap `Data` (serialized) representation, concurrent iteration for migration/replication. | 4 ew |
| F11.3 | HD record stores for IMap (`createService(MapService)` → HD-aware `MapServiceContext`): records, expiry, eviction (LRU/LFU/RANDOM sampling on native), backups, migration, entry processors, queries. | 4 ew |
| F11.4 | HD ICache record store (JCache semantics, eviction). | 2 ew |
| F11.5 | HD indexes: off-heap sorted (B+tree) and hash indexes; config validation (no BITMAP on NATIVE, matching upstream). | 3 ew |
| F11.6 | Member-side HD Near Cache. | 1 ew |
| Tests/perf | Upstream map/cache suites parameterised with `InMemoryFormat.NATIVE`, leak detector (allocator accounting in tests), JMH, a 100 GB GC-pause benchmark. | incl. |

---

## Phase 6 — Persistence P0 (M6): `hecaton-ext-persistence`

| Step | Work | Est. |
|---|---|---|
| F10.1 | **Log-structured store** (shared engine): per-partition-thread append-only chunk files, record/tombstone format with CRC, prefix-tombstones for bulk deletes, background compaction (garbage-ratio driven), `fsync` policy, mmap/FFM I/O, on-heap and native (HD) index of keys → file offsets. | 6 ew |
| F10.2 | Integration with IMap/ICache record stores (on-heap and HD): write-through on mutation, backup replicas persisted, partition migration hand-off. | 3 ew |
| F10.3 | **Cluster metadata persistence:** member UUID/address, member list, partition table + version, cluster state, cluster version. | 2 ew |
| F10.4 | **Coordinated restart:** validation phase (member list/partition table agreement), parallel data load, `ClusterDataRecoveryPolicy` (FULL_RECOVERY_ONLY / PARTIAL_RECOVERY_MOST_RECENT / PARTIAL_RECOVERY_MOST_COMPLETE), force start, partial start, `auto-remove-stale-data`, rejoin of a restarted member with a new address (`rebalance-delay`), timeouts. | 4 ew |
| F10.5 | **Hot backup:** `PersistenceService#backup()` with consistent cut per partition, backup dir layout, restore by pointing `base-dir` at the backup. | 1.5 ew |
| F10.6 | Management ops: `ClusterState` changes, REST/CLI (`hc-cluster-admin` force/partial start, backup). | 1 ew |
| Tests | kill -9 matrix during writes/compaction/migration, torn-write injection, full cluster restart with 10⁷ entries, partial start scenarios, backup/restore. Nightly; must pass 30 consecutive runs before persistence leaves "preview". | 2.5 ew |

---

## Phase 7 — WAN replication P0 (M7): `hecaton-ext-wan`

| Step | Work | Est. |
|---|---|---|
| F13.1 | `WanReplicationService` implementation (`createService`), publisher lifecycle per `WanReplicationConfig`. | 1 ew |
| F13.2 | **Batch publisher:** per-partition event queues (with backup replicas), batching by size/time, ack types (`ACK_ON_RECEIPT`, `ACK_ON_OPERATION_COMPLETE`), queue-full behaviour (DISCARD / THROW_EXCEPTION / THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE), target endpoints via static list or discovery SPI, endpoint health/failover, TLS through the WAN endpoint qualifier. | 3 ew |
| F13.3 | **Consumer:** WAN endpoint (advanced network `WAN` server socket config), apply with merge policies, `persistWanReplicatedData`, custom consumers. | 1.5 ew |
| F13.4 | Management ops: pause/stop/resume, clear queues, dynamic add of WAN config, publisher state metrics, REST/CLI. | 1 ew |
| Tests | Two- and three-cluster topologies (active/passive, active/active with merge policies), endpoint failure, queue-full behaviour. | 1.5 ew |

(WAN sync — full and Merkle-tree delta, building on the existing `merkletree` package — and
consistency check are P1, in M9.)

---

## Phase 8 — Hecaton 1.0 (M8)

- Performance pass against upstream CE 5.7 (§10 targets), HD and persistence tuning.
- External security review of `hecaton-ext-tls` and `hecaton-ext-security`.
- Docs site: getting started, migration guide (Hazelcast CE → Hecaton, Hazelcast EE →
  Hecaton feature mapping), feature reference, operations guide.
- Release process (needs `hecatongrid.org` registered, D7): signed artifacts to Maven Central
  under `org.hecatongrid`, Docker image,
  SBOM (CycloneDX), release notes generated from conventional commits. No Helm chart
  or Kubernetes operator (D9).
- If upstream has released a newer minor by then, run the upstream sync (below) first and
  release 1.0 on that base.

---

## Phase 9 — 1.x (P1, M9)

| Feature | Notes | Est. |
|---|---|---|
| F2 LDAP & Kerberos realms | JNDI LDAP (virtual threads), GSS-API/Kerberos login module, role mapping | 3 ew |
| F4 Audit log | `AuditlogService`, event types, log4j/JUL sink | 1 ew |
| F5 Socket interceptor | member + client | 0.5 ew |
| F9 CP persistence | Raft log + snapshots on the shared store; restart and recovery of CP members | 4 ew |
| F10 Encryption at rest | AES-GCM per chunk, Java KeyStore and HashiCorp Vault key sources, key rotation | 3 ew |
| F11 Client HD Near Cache | FFM allocator has to be usable from the Java 17 client: ship a Java 17 Unsafe-free fallback (on-heap) and enable native near cache on Java 22+ clients only (multi-release jar) | 2 ew |
| F13 WAN sync & consistency check | Full sync, Merkle-tree delta sync, consistency check | 4 ew |
| F14 Rolling upgrades | `supportedVersions={N-1,N}`, auto-upgrade; becomes testable with the second Hecaton minor; upgrade test N-1→N in CI | 3 ew |
| F16 Dynamic config persistence, config reload/update | Persist dynamic config changes back to the declarative file; reload/update diff engine | 3 ew |
| F17 User Code Namespaces | Namespace service, per-namespace classloaders, resource upload, integration points | 4 ew |
| Slim client jar (§8.3 phase 2) | Reachability-based packaging + ArchUnit rules | 2 ew |

---

## Phase 10 — 2.x (P2, M10)

| Feature | Notes | Est. |
|---|---|---|
| F12 Tiered Storage | Memory tier (HD) + disk tier on the log-structured store, `LocalDeviceConfig` | 8 ew |
| F18 Jet lossless restart, isolated jobs, job upgrade / exported snapshots | Persist Jet snapshot maps through F10; job placement isolation; snapshot export/import | 6 ew |
| F19 SQL catalog persistence | Persist the `__sql.catalog` map via F10 (lives in an Apache module; the HCL `hecaton-sql` only reads it) | 1 ew |
| F20 Vector collection | Service + partitioned HNSW index on an Apache-2.0 ANN library (e.g. jvector), metrics, backups, client codecs (already in core) | 10 ew |

---

## Continuous workstream — upstream sync

For each upstream CE release (minor) and for upstream security fixes:

1. `tools/upstream/import-snapshot.sh vX.Y.Z` adds the new tag as one snapshot commit on `upstream/main`.
2. `tools/rebrand --tag vX.Y.Z` → commit to `rebrand/vX.Y.Z`.
3. Merge into a `sync/vX.Y.Z` branch from `main`. Resolve conflicts only in files listed in
   `docs/upstream-patches.md`. Run the SPI contract tests.
4. Re-run the CP 5.4→current API diff and every `hecaton-ext-*` suite.
5. Release `X.Y.Z-hc.1`.
6. Security fixes between releases: `tools/rebrand --commit <sha>` produces a renamed patch
   that is cherry-picked onto the maintenance branch; release `X.Y.Z-hc.N`.

Budget: ~1–2 ew per upstream minor.

---

## Immediate next steps (first PRs)

1. **PR #1 — Foundation (done, `feature/m0-foundation`):** Apache 2.0 `LICENSE` + `NOTICE`,
   `CONTRIBUTING.md` with the clean-room policy, PR template, `SECURITY.md`, `CODEOWNERS`,
   CI skeleton on JDK 25.
2. **PR #2 — Upstream import (done):** `upstream/main` holds the `v5.7.0` snapshot, created
   by `tools/upstream/import-snapshot.sh` with `tools/upstream/excluded-paths.txt`.
3. **PR #3 — `tools/rebrand` v1** + `rename-table.yaml` + keep-list + verification checks.
4. **PR #4 — `rebrand/v5.7.0` merged into `main`**, JDK 25 build fixes, baseline test
   comparison report.
5. **PR #5 — Client matrix (17/21/25) + official-client compat smoke tests.**
6. **PR #6 — Migration tooling** (OpenRewrite recipe + CLI + sample apps).
7. **PR #7 — Extension framework** (M2).
