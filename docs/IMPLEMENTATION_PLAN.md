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
| **M11** Load protection (implemented, in review) | F23: immutable (no-update) maps with shared payloads, pooled connection buffers and back pressure, workload isolation, partitioned queues ([design](design/load-protection.md)) | per phase, see the design | 20–26 ew (phases 3 and 4 optional) |

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
| M11 Load protection | `feature/m11-load-protection` |

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
| **Keep-list** (must *not* be renamed; wire- or data-visible) | Distributed-object service names (`hz:impl:mapService`, `hz:impl:*`, `hz:core:*`, `hz:raft:*` …) that official clients send in protocol messages; the client protocol bytes (`CP2`; the member protocol bytes became `HCC` so Hazelcast members cannot join, spec NG1); codec IDs; the server version string format; factory/class IDs; Compact type names; internal object names (`__sql.catalog`, `__jet.*`); copyright headers, `LICENSE`, `NOTICE`, third-party attributions. The keep-list is tested: a fixture of official-client traffic is replayed against the renamed member. |
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
5. **Product identity (done):** project version `0.1.0-SNAPSHOT`; `BuildInfo` reports the
   product version separately from the codebase version `5.7.0` that members and clients
   negotiate with (spec §3.2); startup line "Hecaton Platform 0.1.0 (…, based on Hazelcast
   5.7.0, cluster version 5.7)"; distribution scripts fixed (`hc-start`); container image
   `hecaton/hecaton` (`distribution/src/docker/Dockerfile`, Temurin 25 JRE, slim
   distribution), built and smoke-tested by the CI build job (`tools/ci/image-test.sh`).

Est.: 3 ew.

### 1.3 Client library 17/21/25 (phase 1 of §8.3)

- Publish `uk.org.hecaton:hecaton-client` (done: a POM that depends on the Java 17 core jar;
  the slim client jar is M9).
- Add a client-test profile (the upstream `client` test packages plus new smoke tests) to
  `client-matrix.yml` on Temurin 17, 21 and 25.

Est.: 0.5 ew.

### 1.4 Compatibility smoke tests

| Test | Status |
|---|---|
| Official **Hazelcast** clients against a Hecaton member: Java 5.5/5.6/5.7, Python, Node.js, Go and .NET, each with IMap + entry listener, queue, topic, SQL and Compact (`compat-tests/`, CI job `compat`). The member runs as a plain JVM process rather than a container image: the protocol coverage is the same, and the image comes with the distribution work (M8). | Done |
| Negative test: a Hazelcast 5.7 member trying to join a Hecaton cluster is refused with the documented message. It used to join: the member protocol now opens with `HCC` instead of `HZC` (spec §5.4). | Done |
| Kubernetes job (nightly, kind): 3 Hecaton members form a cluster through Kubernetes discovery in DNS and API modes using the renamed `kubernetes-rbac.yaml`, survive a rolling restart of the StatefulSet, and a client pod connects (`kubernetes-tests/`, workflow `kubernetes.yml`). | Done |

Est.: 2 ew.

### 1.5 Migration tooling (§5)

| Deliverable | Work | Status |
|---|---|---|
| `hecaton-migrate-rewrite` | OpenRewrite recipe `uk.org.hecaton.migrate.FromHazelcast`: renames the `com.hazelcast` types, methods and constants an application uses (derived with the fork's rules, not from a list), string literals with the CLI's rules, Maven/Gradle dependencies. | Done |
| `hecaton-migrate` CLI | Non-Java files: config XML/YAML, `.properties`, env files, Dockerfiles, Helm/K8s manifests, shell scripts; default config file names. Dry run by default, `--apply`, `--check`. Only names Hecaton reads are renamed (spec §5). | Done |
| Acceptance | `migration-tests/` samples, run by the `migration` CI job (`tools/ci/migration-tests.sh`). | Embedded member, client + Jet + SQL, and Spring Boot (the recipe adds `hecaton-spring-boot3`/`-boot4`) done; EE-style config once those features exist (M3+). |
| `hecaton-migrate data` | Client-based copier (Hazelcast cluster → Hecaton cluster). The Hecaton client reads the Hazelcast cluster (same client protocol) and copies IMap, MultiMap and ReplicatedMap entries in serialized form; Compact values go through `GenericRecord` so their schemas reach the target. Tested against a real Hazelcast 5.7 cluster (`compat-tests`, `data-copy`). | Done; ICache open (needs the target cache configuration) |
| Gradle | The recipe changes Gradle dependencies (`ChangeDependency`), but no test or sample covers it yet, and the Spring Boot auto-configuration dependency is only added for Maven. | Open (M8: Gradle migration sample) |

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
| Spring Boot auto-configuration — done | `HecatonAutoConfiguration` in `hecaton-spring-boot3`/`-boot4` (logic shared in `hecaton-spring`, `uk.org.hecaton.spring.autoconfigure`): a member from a `Config` bean or member configuration (`spring.hecaton.config`, `hecaton.config`, default `hecaton.xml`/`.yaml`), else a client from a `ClientConfig` bean or client configuration. `BootHecatonApplicationTest` moved into the Boot modules and passes. Next: the Spring Boot migration sample (1.5). |
| Spring Hibernate XML support — done | `RegionFactoryBeanDefinitionParser` deleted; `<hz:hibernate-region-factory>` now fails with a message. |
| Python 3.14 — moved to M8 | The pinned protobuf 4.25.8 / grpcio 1.73.0 fail on Python 3.14. `grpc.version` and the protobuf version are shared with the Java gRPC extension, so this is the Java protobuf 4.x / gRPC upgrade with regenerated code; part of the M8 dependency refresh. The nightly pins Python 3.12 meanwhile. |
| Deep JSON nesting — done | `JsonParser` reads arrays and objects with an explicit stack instead of recursion; the known failure is gone. |
| Priority queue failover duplicate — fixed in M2 | `QueueMigrationTest#testPromotion` with a priority comparator once saw 101 items instead of 100 after the owner was terminated. Cause: queue statistics (metrics collection, `getLocalQueueStats()`) called `QueueContainer#size()`/`backupSize()` outside the partition thread, and those move the items between the primary queue and the backup map; concurrently with the partition thread this can lose or duplicate items, most likely around a promotion. Statistics now count without moving anything (`QueueStatsReadOnlyTest`); the known-failure entry is removed. |
| Schema hosting | `XmlConfigSchemaLocationTest` needs the XSDs at `https://hecaton.org.uk/schema/...` (M8, D7). |

**M1 exit:** tag `v0.1.0` (CE 5.7.0 parity minus cloud-provider support, renamed, Java 25 build).

---

## Phase 2 — Core cleanup (M2)

With D10 the provider indirection planned earlier (`hecaton-ext-spi`, `FeatureProvider`,
delegating `HecatonNodeExtension`) is not needed: Java 17 features are built directly in
core (spec §7.1).

1. **Edition gates out — done:** the "requires Enterprise" checks in `HecatonNodeExtension`,
   `ConfigValidator`, `Node`, `ChannelInitializerFunction`, `HecatonClientExtension`, the
   cache, WAN, Jet, REST and TPC classes now ask `HecatonFeature#isAvailable()` and fail with
   its message ("X (Fn) is not available in Hecaton yet; it is planned for milestone M…", or
   "not supported; use …"). A feature's entry flips when its implementation lands.
   `BuildInfo#isEnterprise()` stays `false` (not `true` as first planned): it is sent to
   clients in the authentication response, and official clients would then expect
   Enterprise behaviour. The upstream tests that asserted "requires Enterprise" use the new
   messages.
2. **Licensing code out (F21) — done:** `license-key` is accepted and ignored with an INFO
   log, also when set through REST or Management Center; `getLicense()` returns `null` (no
   caller needs a descriptor); instance tracking no longer looks for the Enterprise
   built-in license.
3. **`DefaultNodeExtension` → `HecatonNodeExtension` — done** (constructed directly; the
   priority-list lookup and `NodeExtensionFactory` are gone); same for the client extension.
4. **`MemberModule` SPI — done** (spec §7.2): `uk.org.hecaton.instance.impl.module`, capability
   interfaces for HD, persistence, tiered storage and vector (their operations come with
   the modules), `MemberModules#require` for the "requires `uk.org.hecaton:hecaton-<module>`"
   errors.
5. **Java 25 member check — done** (spec §8.2): `MemberJavaVersion`; the client smoke test
   on JDK 17/21 runs its member on Java 25 and checks that a member refuses an older Java.
6. **F15 client failover — done:** `isClientFailoverSupported() → true`; `FailoverTest`
   covers moving to the second cluster when the first goes down or blocks clients.
7. **`hecaton-member` POM — done:** core plus `hecaton-sql`; member modules join it as they
   appear.
8. **Priority-queue failover duplicate — fixed** (moved from M1, see §1.9).

**M2 exit:** merge to `main`, tag `v0.2.0`.

Est.: 2–3 ew.

---

## Phase 3 — Security P0 (M3): core

| Step | Work | Est. |
|---|---|---|
| F1.1 — JDK factory done | `SSLEngineFactory` implementations: JDK (`BasicSSLContextFactory`-compatible properties: keystore/truststore type/password/path, protocol, ciphers, `mutualAuthentication` REQUIRED/OPTIONAL) and optional OpenSSL via netty-tcnative (Apache 2.0). | 2 ew |
| F1.2 — done | TLS channel handlers in `createChannelInitializerFn` for MEMBER, CLIENT, WAN, REST, MEMCACHE endpoint qualifiers (advanced networking). Handshake timeout, ALPN not required. | 1.5 ew |
| F1.3 — done | Client-side TLS in the client extension, Java 17 compatible. | 1 ew |
| F1.4 — reload done (`keyMaterialDuration`); expiry metrics open | Keystore hot-reload (`keyStoreReload`/file-watch), cert expiry metrics. | 0.5 ew |
| F2.1 — done | `SecurityContext`/`SecurityService`: realms, JAAS login, `Credentials` types (username/password, token, cert), member-to-member authentication, client authentication in `AuthenticationBaseMessageTask`. | 2.5 ew |
| F2.2 — done (simple, TLS cert, default; token via the default module) | Built-in login modules at the renamed EE class names (`docs/ee-class-map.md`): simple, username/password config realm, TLS cert, token. | 1 ew |
| F3.1 — done except `client-block-unmapped-actions` | Permission evaluation for every `PermissionConfig.PermissionType` (map, cache, queue, topic, lock, atomic, executor, SQL, Jet job/connector, CP, user code namespace, vector, config, transaction, all); wildcards and principal matching; `SecurityInterceptor` chain; on-join permission policy (RECEIVE/SEND/NONE). | 2.5 ew |
| Tests — done (TLS conformance `tls-conformance.yml` nightly; fuzzed authentication in `SecurityTest`) | Port of the public permission semantics into tests; TLS conformance (`testssl.sh`) in nightly; fuzzed auth messages. | incl. |

---

## Phase 4 — CP Subsystem P0 (M4): core

| Step | Work | Est. |
|---|---|---|
| F7.1 — done | Import `com/hazelcast/cp/**` (main and test) from `v5.4.0` (Apache 2.0, ~375 main files) into core through `tools/rebrand` (same rename table). Keep the original headers; add provenance to `NOTICE`/`docs/provenance.md`. | 0.5 ew |
| F7.2 — done | Diff the CP public/SPI surface between 5.4.0 and 5.7.0 (`CPSubsystem`, `CPSubsystemManagementService`, `CPSubsystemConfig`, CP group/session APIs, metrics, client codecs) and adapt the ported engine to it. Replace `CPSubsystemStubImpl` (delete the stub). | 2.5 ew |
| F7.3 — done (incl. Management Center CP messages) | Register CP client message tasks (codecs exist in core), CP membership REST/CLI (`hc-cluster-cp-admin`). | 1 ew |
| F8 — done (purge on members; no client message for it) | `CPMap` new implementation: Raft-replicated KV state machine with the documented ops and size limits, snapshotting, client proxy, config (`CPMapConfig`). | 2.5 ew |
| Tests — done (`CPLinearizabilityTest`; nemesis: Raft partitions, leader isolation, CP member replacement) | Ported 5.4 CP tests (~114 files) + CPMap tests + linearizability harness (Porcupine-style checker over recorded histories, nemesis: partitions, pauses, kills). | 2 ew |

---

## Phase 5 — High-Density memory P0 (M5): `hecaton-hd`

| Step | Work | Est. |
|---|---|---|
| F11.1 — done (thread-local pools; free blocks of other pools are used when no page fits) | **FFM memory layer** (Java 25): `MemorySegment`-based `MemoryAllocator` with STANDARD (malloc-like via `Arena`) and POOLED (power-of-two buddy allocator with page/min-block sizes per `NativeMemoryConfig`), metadata space, thread-local allocation caches, native OOM handling, `MemoryStats`. | 4 ew |
| F11.2 — done (sorted open-addressing table: iteration cursors survive resizes and changes) | Off-heap data structures: open-addressing hash map with off-heap keys/values, slab-allocated records, off-heap `Data` (serialized) representation, concurrent iteration for migration/replication. | 4 ew |
| F11.3 — done (expiry metadata of entries with a TTL stays on the heap; native max-size policies; forced eviction on native OOM) | HD record stores for IMap (`createService(MapService)` → HD-aware `MapServiceContext`): records, expiry, eviction (LRU/LFU/RANDOM sampling on native), backups, migration, entry processors, queries. | 4 ew |
| F11.4 — done | HD ICache record store (JCache semantics, eviction). | 2 ew |
| F11.5 — partial: indexes of `NATIVE` maps are on the heap; BITMAP refused as upstream. Off-heap index stores open | HD indexes: off-heap sorted (B+tree) and hash indexes; config validation (no BITMAP on NATIVE, matching upstream). | 3 ew |
| F11.6 — done (records on the heap, values native) | Member-side HD Near Cache. | 1 ew |
| Tests/perf — suites done (nightly `hd-native-suites`: core's map and query tests with `NATIVE`; allocator accounting in `hecaton-hd`'s tests); JMH and the GC-pause benchmark open | Upstream map/cache suites parameterised with `InMemoryFormat.NATIVE`, leak detector (allocator accounting in tests), JMH, a 100 GB GC-pause benchmark. | incl. |

---

## Phase 6 — Persistence P0 (M6): `hecaton-persistence`

| Step | Work | Est. |
|---|---|---|
| F10.1 — done (per-partition logs written by the partition threads; compaction on the partition thread; key index on the heap) | **Log-structured store** (shared engine): per-partition-thread append-only chunk files, record/tombstone format with CRC, prefix-tombstones for bulk deletes, background compaction (garbage-ratio driven), `fsync` policy, mmap/FFM I/O, on-heap and native (HD) index of keys → file offsets. | 6 ew |
| F10.2 — done (map mutation observer; a record observer in the cache record store; caches created at runtime keep their configuration) | Integration with IMap/ICache record stores (on-heap and HD): write-through on mutation, backup replicas persisted, partition migration hand-off. | 3 ew |
| F10.3 — done (member UUID, member list, partition table, cluster state) | **Cluster metadata persistence:** member UUID/address, member list, partition table + version, cluster state, cluster version. | 2 ew |
| F10.4 — done; Kubernetes-managed restarts open | **Coordinated restart:** validation phase (member list/partition table agreement), parallel data load, `ClusterDataRecoveryPolicy` (FULL_RECOVERY_ONLY / PARTIAL_RECOVERY_MOST_RECENT / PARTIAL_RECOVERY_MOST_COMPLETE), force start, partial start, `auto-remove-stale-data`, rejoin of a restarted member with a new address (`rebalance-delay`), timeouts. | 4 ew |
| F10.5 — done | **Hot backup:** `PersistenceService#backup()` with consistent cut per partition, backup dir layout, restore by pointing `base-dir` at the backup. | 1.5 ew |
| F10.6 — done (the existing REST, MC and `hc-cluster-admin` operations) | Management ops: `ClusterState` changes, REST/CLI (`hc-cluster-admin` force/partial start, backup). | 1 ew |
| Tests — kill -9 suite (single member and cluster, during writes and compaction), restart, partial/force start and backup tests; on every build and nightly. 10⁷-entry restart and migration-time kills open | kill -9 matrix during writes/compaction/migration, torn-write injection, full cluster restart with 10⁷ entries, partial start scenarios, backup/restore. Nightly; must pass 30 consecutive runs before persistence leaves "preview". | 2.5 ew |

---

## Phase 7 — WAN replication P0 (M7): core

| Step | Work | Est. |
|---|---|---|
| F13.1 — done | `WanReplicationService` implementation (`createService`), publisher lifecycle per `WanReplicationConfig`. | 1 ew |
| F13.2 — done (one batch in flight per publisher and member) | **Batch publisher:** per-partition event queues (with backup replicas), batching by size/time, ack types (`ACK_ON_RECEIPT`, `ACK_ON_OPERATION_COMPLETE`), queue-full behaviour (DISCARD / THROW_EXCEPTION / THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE), target endpoints via static list or discovery SPI, endpoint health/failover, TLS through the WAN endpoint qualifier. | 3 ew |
| F13.3 — done (maps and caches) | **Consumer:** WAN endpoint (advanced network `WAN` server socket config), apply with merge policies, `persistWanReplicatedData`, custom consumers. | 1.5 ew |
| F13.4 — done (the existing API, REST and Management Center operations; adding a config is cluster-wide) | Management ops: pause/stop/resume, clear queues, dynamic add of WAN config, publisher state metrics, REST/CLI. | 1 ew |
| Tests — done (`WanBatchReplicationTest`, `WanNetworkReplicationTest` over TCP) | Two- and three-cluster topologies (active/passive, active/active with merge policies), endpoint failure, queue-full behaviour. | 1.5 ew |

(WAN sync — full and Merkle-tree delta, building on the existing `merkletree` package — and
consistency check are P1, in M9.)

---

## Phase 8 — Hecaton 1.0 (M8)

| Step | Work | Status |
|---|---|---|
| Performance | Pass against Hazelcast CE 5.7 (§10 targets), HD and persistence tuning. | JMH benchmarks against Hazelcast CE 5.7 (`benchmarks/`, `tools/perf/compare.py`, [performance.md](performance.md)); the HD GC-pause benchmark (100 GB) needs a large machine and is open |
| Security review | External review of the TLS and security code. | Brief for the reviewers in [security-review.md](security-review.md); static analysis (CodeQL, OWASP dependency check, SpotBugs/FindSecBugs) weekly in `security.yml`. The review itself needs an external party |
| Docs | Getting started, migration guide (Hazelcast CE → Hecaton, Hazelcast EE → Hecaton feature mapping), feature reference, operations guide. | Done, as Markdown in `docs/` ([index](README.md)); a generated site is optional |
| Release process | Needs `hecaton.org.uk` registered (D7): signed artifacts to Maven Central under `uk.org.hecaton`, Docker image, SBOM (CycloneDX), release notes. Kubernetes deployment is documented with plain manifests (StatefulSet, headless service, `kubernetes-rbac.yaml`); a Helm chart is not planned for 1.0. | Done: `release.yml` on a `v*` tag ([releasing.md](releasing.md)); Maven Central needs the namespace and the repository secrets |
| WAN suite | Two- and three-cluster topologies of the container image (§10). | Done: `wan-tests/`, nightly `wan.yml` |

---

## Phase 9 — 1.x (P1, M9)

| Feature | Notes | Est. |
|---|---|---|
| F2 LDAP & Kerberos realms — done (system/acceptor credentials from another realm open) | JNDI LDAP (virtual threads), GSS-API/Kerberos login module, role mapping | 3 ew |
| F4 Audit log — done | `AuditlogService`, event types, log4j/JUL sink | 1 ew |
| F5 Socket interceptor — done | member + client | 0.5 ew |
| F9 CP persistence — done (in core with its own files, not on the shared store) | Raft log + snapshots on the shared store; restart and recovery of CP members | 4 ew |
| F10 Encryption at rest — done (AES-GCM per record instead of per chunk) | AES-GCM per chunk, Java KeyStore and HashiCorp Vault key sources, key rotation | 3 ew |
| F11 Client HD Near Cache — done (`hecaton-hd` on Java 25 clients through a `ServiceLoader` SPI, on-heap fallback otherwise; no multi-release jar) | FFM allocator has to be usable from the Java 17 client: ship a Java 17 Unsafe-free fallback (on-heap) and enable native near cache on Java 22+ clients only (multi-release jar) | 2 ew |
| F13 WAN sync & consistency check — done (maps) | Full sync, Merkle-tree delta sync, consistency check | 4 ew |
| F14 Rolling upgrades — done (tested with artificial versions until a second cluster version exists) | `supportedVersions={N-1,N}`, auto-upgrade; becomes testable with the second Hecaton minor; upgrade test N-1→N in CI | 3 ew |
| F16 Dynamic config persistence, config reload/update — done | Persist dynamic config changes back to the declarative file; reload/update diff engine | 3 ew |
| F17 User Code Namespaces — done | Namespace service, per-namespace classloaders, resource upload, integration points | 4 ew |
| Slim client jar (§8.3 phase 2) — investigated, not shipped | `tools/client-jar/reachability.py` (jdeps): 74% of core's classes are reachable from the client entry points, 90% with the serializer hooks and other service files the client loads, so a reachability-built jar would be about 5% smaller while risking missing classes on reflective paths. A useful client jar first needs client and member code decoupled (shared serializer hooks, `HecatonInstanceFactory`, CP proxy helpers in `RaftService`); `--gateways` lists where to start. `hecaton-client` stays the core jar | 2 ew |

---

## Phase 10 — 2.x (P2, M10)

| Feature | Notes | Est. |
|---|---|---|
| F12 Tiered Storage — done (`hecaton-tiered`) | Memory tier (HD native memory) + disk tier, `LocalDeviceConfig`. The disk tier is an append-only file per partition with compaction rather than the persistence store: it is a cache that does not survive restarts, so it needs no recovery | 8 ew |
| F18 Jet lossless restart, isolated jobs, job upgrade / exported snapshots — done (export from running jobs only) | Persist Jet snapshot maps through F10; job placement isolation; snapshot export/import | 6 ew |
| F19 SQL catalog persistence — done | Persist the `__sql.catalog` map via F10 (lives in an Apache module; the HCL `hecaton-sql` only reads it) | 1 ew |
| F20 Vector collection — done (own HNSW; no metrics yet) | Service + partitioned HNSW index, backups, client message tasks (codecs already in core). The HNSW graph is written for Hecaton rather than taken from an ANN library: the index has to follow the backing map's record store partition by partition, which a small graph per partition replica does directly | 10 ew |

---

## Phase 11 — Load protection (M11, implemented, in review)

Plan under review: [design/load-protection.md](design/load-protection.md). Server-side, off by
default (`LoadProtectionConfig#setEnabled`). Phases: 0 measure (allocation harness done:
`tools/perf/hot-read-allocation.sh`), 1 immutable maps, reads between members and bounds (first),
2 workload isolation, 3 other formats (optional), 4 partitioned queue (nice to have), 5 hardening.

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
   - official-client compatibility tests and the Hazelcast-member refusal (1.4);
   - next: the open items in 1.9 (Spring Boot auto-configuration first, then its migration
     sample), `hecaton-migrate data` (1.5), the Kubernetes job (1.4), tag `v0.1.0`.
3. **M2 Core cleanup**, then M3–M7 in parallel.
