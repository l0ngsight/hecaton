# Hecaton — Implementation Plan

Companion to [`SPEC.md`](SPEC.md). Section references (§) point to the spec.

| | |
|---|---|
| Forked from | Hazelcast CE `v5.7.0`, once; maintained independently afterwards (D10, spec §9) |
| Target for 1.0 | All **P0** features from §6 on the Hecaton code base |
| Effort unit | engineer-weeks (ew), rough order of magnitude |

---

## Milestones at a glance

| Milestone | Content | Exit criteria | Est. |
|---|---|---|---|
| **M0** Foundation | Repo license, upstream snapshot import, CI on JDK 25, contribution rules | CI green on an empty build; decisions D4–D6, D8 confirmed (D7 tracked separately) | 1–2 ew |
| **M1** Rebranded CE (`0.1.0`) | `tools/rebrand`, renamed code base, cloud-provider removal (D9), retire sync tooling (D10), JDK 25 build, client 17/21/25 matrix, migration tooling | Inherited test suite at parity with Hazelcast 5.7.0 on JDK 25 (minus removed cloud-provider tests); official clients connect; migration acceptance test passes | 10–12 ew |
| **M2** Core cleanup | Remove edition gates and licensing code, `MemberModule` SPI, Java 25 member check, client failover | No "requires Enterprise" path left; "missing module" errors tested | 2–3 ew |
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
   maintenance (security fixes, dependency updates) runs continuously from M1 onward
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
| M2 Core cleanup | `feature/m2-core-cleanup` |
| M3 Security P0 | `feature/m3-security` |
| M4 CP Subsystem P0 | `feature/m4-cp-subsystem` |
| M5 HD memory P0 | `feature/m5-hd-memory` |
| M6 Persistence P0 | `feature/m6-persistence` |
| M7 WAN P0 | `feature/m7-wan-replication` |
| M8 Hecaton 1.0 | `feature/m8-release-1.0` |
| M9 1.x (P1) | `feature/m9-p1-features` |
| M10 2.x (P2) | `feature/m10-p2-features` |

`upstream/main` (the 5.7.0 snapshot) and `rebrand/v5.7.0` (its renamed form) are kept
read-only as the record of how the fork was made (spec §9). No new branches of either kind
are created.

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
   `Upstream-Excludes-SHA256` trailers). Files are byte-identical to upstream. Upstream's
   `.github/` (its CI would run here) and the self-contained cloud-provider code (D9), 197
   files, were left out. This happened once; the import script was retired with D10.
4. **CI skeleton** (GitHub Actions, `.github/workflows/ci.yml`; the logic lives in
   `tools/ci/*.sh` so later milestones only change scripts):
   - `hygiene`: Apache `LICENSE`/`NOTICE` present, no Hazelcast Enterprise artifacts or
     repositories in build files, no `sun.misc.Unsafe` in Hecaton-owned code.
   - `build`: JDK 25 (Temurin). Skips the Maven build until M1 adds `pom.xml`; M1 also turns
     on `setup-java`'s Maven cache, which fails when no `pom.xml` exists.
   - `client`: JDK 17/21/25 matrix (JDK 25 also installed for the toolchain). M1 adds the
     `client-tests` Maven profile it calls.
   - Added in M1: `nightly.yml`, the full test suite in 29 parallel shards (upstream's
     `parallelTest` level) with a report that separates upstream-baseline, triaged and new
     failures (`tools/ci/nightly.py`, `docs/test-baseline/`, `tools/ci/known-test-failures.txt`),
     and the rebrand determinism job in `ci.yml` (both upstream-specific parts were retired
     with D10). Crash-consistency, multi-cluster and official-client suites join the
     nightly as their milestones land.
5. **Contribution rules:** `CONTRIBUTING.md` with the clean-room policy (§11.3), DCO
   sign-off, PR template with a provenance checkbox, `CODEOWNERS`, `SECURITY.md`.

---

## Phase 1 — Rebrand tool and renamed baseline (M1)

### 1.1 `tools/rebrand`

A standalone Java 25 CLI in `tools/rebrand/`. Deterministic and idempotent:
`rebrand(upstreamTree) → hecatonTree`. It was run once, on `v5.7.0` (D10). Its text engine
and the rename table are now the base of `hecaton-migrate` (1.5).

| Component | Work |
|---|---|
| `rename-table.yaml` | Single source of truth for §5: package prefix, identifier rules (`Hazelcast`→`Hecaton`, `hazelcast`→`hecaton`), file/path renames, property/env prefixes, XML namespaces, Maven coordinates, script names, metric/JMX/thread prefixes. |
| **Keep-list** (must *not* be renamed; wire- or data-visible) | Distributed-object service names (`hz:impl:mapService`, `hz:impl:*`, `hz:core:*`, `hz:raft:*` …) that official clients send in protocol messages; protocol magic bytes (`HZC`, `CP2`); codec IDs; the server version string format; factory/class IDs; Compact type names; internal object names (`__sql.catalog`, `__jet.*`); copyright headers, `LICENSE`, `NOTICE`, third-party attributions. The keep-list is tested: a fixture of official-client traffic is replayed against the renamed member. |
| Java transformer | Lexical, token-aware rewriting (identifiers, string literals, comments, `import`/`package` lines) driven by the table. Type attribution is not needed because all rules are prefix/substring based. Also covers `META-INF/services/*` file names and contents, reflection strings (e.g. `EXTENSION_PRIORITY_LIST`), `Automatic-Module-Name` (`com.hazelcast.core` → `uk.org.hecaton.core`). |
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
     part of this step and list them in `docs/changes-from-hazelcast.md`.
3. **Build on JDK 25:**
   - Add a Maven toolchains config; set `jdk.version`-style properties per module:
     `hecaton.release.client=17`, `hecaton.release.member=25`.
   - Keep `--release 17` for every renamed upstream module (§8.1).
   - Upgrade build plugins and test libraries only where JDK 25 needs it (Mockito/ByteBuddy,
     SpotBugs, Checkstyle, JaCoCo, Surefire `argLine` with
     `--sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED`).
   - Per-module `enforceBytecodeVersion` (17 or 25) and a rule that forbids 17-targeted
     modules from depending on 25-targeted ones.
4. **Baseline comparison (done):** the *upstream* `v5.7.0` suite and the *renamed* suite
   ran on JDK 25 in the nightly. Failures inherited from 5.7.0 are frozen in
   `docs/test-baseline/upstream-v5.7.0-jdk25.txt`; Hecaton-only failures are fixed or
   triaged in `tools/ci/known-test-failures.txt`. Inherited failures are fixed over time
   like any other bug.
5. **Product identity:** `BuildInfo` (Hecaton version, "based on Hazelcast 5.7.0", D11),
   startup banner, `hecaton-default.xml`, distribution (`hc-*` scripts), Docker image
   `hecaton/hecaton:0.1.0` (Temurin 25 JRE base).

Est.: 3 ew.

### 1.3 Client library 17/21/25 (phase 1 of §8.3)

- Publish `uk.org.hecaton:hecaton-client`, which at this stage re-publishes the Java 17 core
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
- Kubernetes job (nightly, kind): 3 Hecaton members form a cluster through Kubernetes
  discovery in DNS and API modes using the renamed `kubernetes-rbac.yaml`, survive a
  rolling restart of the StatefulSet, and a client pod connects.

Est.: 2 ew.

### 1.5 Migration tooling (§5)

| Deliverable | Work | Status |
|---|---|---|
| `hecaton-migrate-rewrite` | OpenRewrite recipe `uk.org.hecaton.migrate.FromHazelcast`: renames the `com.hazelcast` types, methods and constants an application uses (derived with the fork's rules, not from a list), string literals with the CLI's rules, Maven/Gradle dependencies. | Done |
| `hecaton-migrate` CLI | Non-Java files: config XML/YAML, `.properties`, env files, Dockerfiles, Helm/K8s manifests, shell scripts; default config file names. Dry run by default, `--apply`, `--check`. Only names Hecaton reads are renamed (spec §5). | Done |
| Acceptance | `migration-tests/` samples, run by the `migration` CI job (`tools/ci/migration-tests.sh`). | Embedded member and client + Jet + SQL done; Spring Boot after `HecatonAutoConfiguration` (1.9); EE-style config once those features exist (M3+). |
| `hecaton-migrate data` | Client-based copier (Hazelcast cluster → Hecaton cluster) for IMap/ICache/MultiMap/ReplicatedMap, using official Hazelcast client + Hecaton client in one JVM. | Open |
| Gradle | The recipe changes Gradle dependencies, but no test or sample covers it yet. | Open |

Est.: 2.5 ew.

### 1.6 Remove cloud-provider support (D9, spec NG6) — done

The self-contained plugin and connector directories were left out at import. M1 deletes
what the rest of the code base references. With D10 there is no patch budget, so the
removal is complete rather than minimal:

| Area | Work |
|---|---|
| Config model | Delete `AwsConfig`, `AzureConfig`, `GcpConfig`, `EurekaConfig`, `ClientCloudConfig` and their accessors on `JoinConfig`, `ClientNetworkConfig`, `WanBatchPublisherConfig`, `ClientFailoverConfig`; trim `AliasedDiscoveryConfigUtils` & co. to Kubernetes. `KubernetesConfig` stays. |
| Config loading | XML/YAML parsers, config generators, XSD/JSON schemas (current versions), Spring XML support, `HC_` env overrides: drop the elements. The parsers recognise the old element names only to throw the targeted NG6 error. |
| Client protocol | Keep the wire layout of `AwsConfigCodec` & co. so official clients' dynamic WAN config messages still decode (G3); the decoded cloud sections are rejected if enabled. |
| Member/client runtime | Delete `HecatonCloudDiscovery`/`ViridianAddressProvider` and the cloud branches in `ClusterDiscoveryServiceBuilder`; remove the cloud-provider entries from `META-INF/services/...DiscoveryStrategyFactory` (Kubernetes stays); management DTOs. |
| Build & distribution | Remove the excluded modules from `extensions/pom.xml`, `hadoop-dist/pom.xml`, `distribution` (done); check `hadoop-all` for bundled cloud file systems. |
| Tests & docs | Delete tests of removed features; add tests for the NG6 config error; note the removal in `docs/changes-from-hazelcast.md`. |

Est.: 1.5–2 ew.

### 1.7 Upstream top-level files

Upstream's `README.md`, `LICENSE`, `NOTICE`, `CONTRIBUTING.md`, `SECURITY.md` and
`codecov.yml` were moved to `docs/upstream/` by rebrand, for reference.

### 1.8 Retire upstream-sync tooling (D10)

| Item | Action |
|---|---|
| `tools/upstream/` (snapshot import, excluded paths) | Delete; the 5.7.0 import is recorded on `upstream/main`. |
| `tools/rebrand/rebrand.sh`, `tools/ci/check-rebrand.sh`, the `rebrand` CI job | Delete. `Rebrand.java` and `rename-table.yaml` stay for `hecaton-migrate`. |
| Nightly `suite=upstream` | Delete; the 5.7.0 baseline is frozen. |
| `docs/upstream-patches.md` | Becomes `docs/changes-from-hazelcast.md`: the list of differences from Hazelcast 5.7.0 that migrating users or maintainers need to know. No budget. |
| Old-version compatibility tests (`HecatonStarter`, `get-hz-3`) | Delete; they load real Hazelcast releases next to Hecaton, which is not a goal (NG1). |
| Rename table protections that only served re-runs on new upstream tags | Keep for now; they also document what `hecaton-migrate` must leave alone. |

### 1.9 Open items found while building M1

| Item | Notes |
|---|---|
| Spring Boot auto-configuration | Spring Boot's own Hazelcast auto-configuration only reacts to `com.hazelcast` classes, so a Hecaton app gets no `HecatonInstance` bean (`BootHecatonApplicationTest`, known nightly failure). Ship `HecatonAutoConfiguration` (spec §5.2). |
| Spring Hibernate XML support | `RegionFactoryBeanDefinitionParser` serves the removed Hibernate integration; delete it. |
| Python 3.14 | The pinned protobuf 4.25.8 / grpcio 1.73.0 fail on Python 3.14; bump them (the nightly pins Python 3.12 meanwhile). |
| Deep JSON nesting | `JsonParser` can overflow the stack before its 1,000-level limit on JDK 25 (known nightly failure); bound the recursion depth. |
| Priority queue failover duplicate | `QueueMigrationTest#testPromotion` with a priority comparator once saw 101 items instead of 100 after the owner was terminated (nightly 2026-10-19; did not reproduce in 60 local runs). Find out how the promoted backup gets a duplicate item and fix it (known nightly failure meanwhile). |
| Schema hosting | `XmlConfigSchemaLocationTest` needs the XSDs at `https://hecaton.org.uk/schema/...` (M8, D7). |

**M1 exit:** tag `v0.1.0` (CE 5.7.0 parity minus cloud-provider support, renamed, Java 25 build).

---

## Phase 2 — Core cleanup (M2)

With D10 the provider indirection planned earlier (`hecaton-ext-spi`, `FeatureProvider`,
delegating `HecatonNodeExtension`) is not needed: Java 17 features are built directly in
core (spec §7.1).

1. **Edition gates out:** delete the "requires Enterprise" checks in `DefaultNodeExtension`,
   `ConfigValidator`, `Node`, `ClientEndpointImpl`, `ChannelInitializerFunction`,
   `DefaultClientExtension`, etc. Until a feature lands, its config fails with a precise
   "not implemented yet (Fn)" message instead. `BuildInfo#isEnterprise()` becomes a
   deprecated method returning `true`; the Enterprise build-properties lookup goes.
   Rewrite the upstream tests that assert "requires Enterprise".
2. **Licensing code out (F21):** `license-key` accepted and ignored with an INFO log;
   `getLicense()` returns a "community, unlimited" descriptor; remove the license-related
   REST/CLI paths' Enterprise branches.
3. **`DefaultNodeExtension` → `HecatonNodeExtension`** (one class, no priority list
   lookup); same for the client extension.
4. **`MemberModule` SPI** (spec §7.2) with the capability interfaces for HD, persistence,
   tiered storage and vector, and the "requires `uk.org.hecaton:hecaton-<module>`" errors.
5. **Java 25 member check** (spec §8.2).
6. **F15 client failover:** `isClientFailoverSupported() → true`, plus failover tests with
   two clusters.
7. **`hecaton-member` POM** that pulls in every member module as it appears.

Est.: 2–3 ew.

---

## Phase 3 — Security P0 (M3): core

| Step | Work | Est. |
|---|---|---|
| F1.1 | `SSLEngineFactory` implementations: JDK (`BasicSSLContextFactory`-compatible properties: keystore/truststore type/password/path, protocol, ciphers, `mutualAuthentication` REQUIRED/OPTIONAL) and optional OpenSSL via netty-tcnative (Apache 2.0). | 2 ew |
| F1.2 | TLS channel handlers in `createChannelInitializerFn` for MEMBER, CLIENT, WAN, REST, MEMCACHE endpoint qualifiers (advanced networking). Handshake timeout, ALPN not required. | 1.5 ew |
| F1.3 | Client-side TLS in the client extension, Java 17 compatible. | 1 ew |
| F1.4 | Keystore hot-reload (`keyStoreReload`/file-watch), cert expiry metrics. | 0.5 ew |
| F2.1 | `SecurityContext`/`SecurityService`: realms, JAAS login, `Credentials` types (username/password, token, cert), member-to-member authentication, client authentication in `AuthenticationBaseMessageTask`. | 2.5 ew |
| F2.2 | Built-in login modules at the renamed EE class names (`docs/ee-class-map.md`): simple, username/password config realm, TLS cert, token. | 1 ew |
| F3.1 | Permission evaluation for every `PermissionConfig.PermissionType` (map, cache, queue, topic, lock, atomic, executor, SQL, Jet job/connector, CP, user code namespace, vector, config, transaction, all); wildcards and principal matching; `SecurityInterceptor` chain; on-join permission policy (RECEIVE/SEND/NONE). | 2.5 ew |
| Tests | Port of the public permission semantics into tests; TLS conformance (`testssl.sh`) in nightly; fuzzed auth messages. | incl. |

---

## Phase 4 — CP Subsystem P0 (M4): core

| Step | Work | Est. |
|---|---|---|
| F7.1 | Import `com/hazelcast/cp/**` (main and test) from `v5.4.0` (Apache 2.0, ~375 main files) into core through `tools/rebrand` (same rename table). Keep the original headers; add provenance to `NOTICE`/`docs/provenance.md`. | 0.5 ew |
| F7.2 | Diff the CP public/SPI surface between 5.4.0 and 5.7.0 (`CPSubsystem`, `CPSubsystemManagementService`, `CPSubsystemConfig`, CP group/session APIs, metrics, client codecs) and adapt the ported engine to it. Replace `CPSubsystemStubImpl` (delete the stub). | 2.5 ew |
| F7.3 | Register CP client message tasks (codecs exist in core), CP membership REST/CLI (`hc-cluster-cp-admin`). | 1 ew |
| F8 | `CPMap` new implementation: Raft-replicated KV state machine with the documented ops and size limits, snapshotting, client proxy, config (`CPMapConfig`). | 2.5 ew |
| Tests | Ported 5.4 CP tests (~114 files) + CPMap tests + linearizability harness (Porcupine-style checker over recorded histories, nemesis: partitions, pauses, kills). | 2 ew |

---

## Phase 5 — High-Density memory P0 (M5): `hecaton-hd`

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

## Phase 6 — Persistence P0 (M6): `hecaton-persistence`

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

## Phase 7 — WAN replication P0 (M7): core

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

- Performance pass against Hazelcast CE 5.7 (§10 targets), HD and persistence tuning.
- External security review of the TLS and security code.
- Docs site: getting started, migration guide (Hazelcast CE → Hecaton, Hazelcast EE →
  Hecaton feature mapping), feature reference, operations guide.
- Release process (needs `hecaton.org.uk` registered, D7): signed artifacts to Maven Central
  under `uk.org.hecaton`, Docker image,
  SBOM (CycloneDX), release notes generated from conventional commits. Kubernetes
  deployment is documented with plain manifests (StatefulSet, headless service,
  `kubernetes-rbac.yaml`); a Helm chart is not planned for 1.0.

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

## Continuous workstream — maintenance

Hecaton owns its whole code base (D10):

1. **Security:** advisory process per `SECURITY.md`; watch Hazelcast CE's public CVEs and
   release notes for issues in inherited code; patch releases `X.Y.Z`.
2. **Dependencies:** Renovate/Dependabot for Maven and GitHub Actions, grouped weekly,
   gated by CI and the nightly.
3. **Upstream fixes:** port individual Apache-2.0 fixes from Hazelcast CE by hand when they
   apply, citing the upstream commit.
4. **Inherited failures:** work down `docs/test-baseline/` like any other bug list.

Budget: ~1 ew per month, more after 1.0.

---

## Immediate next steps

1. **M0 Foundation (done, merged):** Apache 2.0 `LICENSE` + `NOTICE`, `CONTRIBUTING.md`
   with the clean-room policy, PR template, `SECURITY.md`, `CODEOWNERS`, CI on JDK 25.
2. **M1 (in progress, `feature/m1-rebranded-ce`):**
   - done: `v5.7.0` snapshot import; `tools/rebrand` (11,680 files rewritten, 11,716 paths
     renamed, 0 leftovers); renamed baseline merged and building on JDK 25; nightly suite
     with a frozen 5.7.0 baseline (73,584 tests, no untriaged failures); client smoke tests
     on JDK 17/21/25; REST `/hazelcast` alias; Kafka Connect test downloads off Hazelcast's
     servers; sync tooling retired (1.8); cloud-provider support removed (1.6); phone home
     removed; migration recipe, CLI and acceptance samples (1.5);
   - next: official-client compat tests (1.4), the open items in 1.9 (Spring Boot
     auto-configuration first, then its migration sample), `hecaton-migrate data` (1.5),
     tag `v0.1.0`.
3. **M2 Core cleanup**, then M3–M7 in parallel.
