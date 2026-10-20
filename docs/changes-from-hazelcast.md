# Changes from Hazelcast 5.7.0

Hecaton was forked once from Hazelcast CE 5.7.0 and is maintained independently (spec §9,
D10). This file lists what differs from Hazelcast 5.7.0 beyond the mechanical rename in
[`tools/rebrand/rename-table.yaml`](../tools/rebrand/rename-table.yaml) (spec §5): first
what migrating applications notice, then a log of the changes made to inherited files while
creating the fork.

Add an entry in the same PR as any change that applications migrating from Hazelcast 5.7
can observe (API, config, wire, REST, metrics, file names, behaviour).

## Differences visible to applications

| Area | Hazelcast 5.7 | Hecaton | Since |
|---|---|---|---|
| Cloud-provider discovery (D9) | `aws`, `azure`, `gcp`, `eureka` join/discovery config (also in WAN batch publishers); client `hazelcast-cloud` and the `hazelcast.client.cloud.discovery.token` property; `AwsConfig`, `GcpConfig`, `AzureConfig`, `EurekaConfig`, `ClientCloudConfig` and their getters/setters | Removed; Kubernetes, TCP/IP, multicast and the Discovery SPI remain. XML/YAML/Spring configs and the client property fail with "Cloud-provider discovery is not supported in Hecaton; use tcp-ip, multicast, kubernetes or a Discovery SPI implementation". Official clients that add a WAN config dynamically still work; an enabled cloud section in it is rejected. | 0.1.0 |
| Spring XML namespace | `http(s)://www.hazelcast.com/schema/spring` | `http(s)://hecaton.org.uk/schema/spring` (both schemes resolve) | 0.1.0 |
| Jet connectors | Kinesis, S3, and the S3/GCS/Azure file systems | Removed (D9) | 0.1.0 |
| External integrations | `hazelcast-hibernate53`, `hazelcast-wm` (separate Hazelcast projects) | Not available; they are built against `com.hazelcast` | 0.1.0 |
| REST paths | `/hazelcast/rest/...`, `/hazelcast/health/...` | `/hecaton/...`; the `/hazelcast/...` paths are still served as aliases | 0.1.0 |
| SQL catalog name | `hazelcast` | `hecaton` (only fully qualified names `hazelcast.public.x` change) | 0.1.0 |
| Spring Boot | Spring Boot's built-in Hazelcast auto-configuration creates the instance (`spring.hazelcast.config`) | Spring Boot's built-in support only knows `com.hazelcast`. Add `hecaton-spring-boot3` or `hecaton-spring-boot4`, whose `HecatonAutoConfiguration` does the same from a `Config`/`ClientConfig` bean, `spring.hecaton.config` or a default `hecaton.xml`/`hecaton-client.xml` (YAML too) | 0.1.0 |
| Member protocol | Members open connections with `HZC` | `HCC`. A Hazelcast member cannot join a Hecaton cluster: its connections are closed and the Hecaton member logs a warning. Clients are not affected (`CP2` is unchanged) | 0.1.0 |
| Usage reporting ("phone home") | Members send usage statistics to `phonehome.hazelcast.com` once a day and probe cloud metadata endpoints to detect the environment; `hazelcast.phone.home.enabled` / `HZ_PHONE_HOME_ENABLED` turn it off | Removed: members send nothing anywhere. `hecaton.phone.home.enabled` is ignored | 0.1.0 |

## Fork log

Changes to inherited files made while creating the fork (M1), beyond what `tools/rebrand`
generated. Generated changes are not listed: they are fully described by the rename table
and the import exclusions (`tools/upstream/excluded-paths.txt`, in git history; removed
with D10). That includes the
eight inherited test jars, which `tools/rebrand` rewrote (entry paths and class-file
constant pools), and the `@Generated` integrity hashes of the client-protocol codecs, which
it recomputed.

| # | Files | Reason | Milestone |
|---|---|---|---|
| 1 | `hecaton-parent/pom.xml` | Removed Hazelcast's private Maven repositories (`repository.hazelcast.com` release/snapshot/security) and `distributionManagement`. Clean room (CONTRIBUTING §1); Hecaton publishes to Maven Central (M8). | M1 |
| 2 | `extensions/pom.xml`, `extensions/hadoop-dist/pom.xml`, `distribution/pom.xml`, `hecaton-coverage-report/pom.xml` | Dropped the cloud-provider modules left out at import (Kinesis, S3, files-azure/gcs/s3) and their dependencies (D9). | M1 |
| 3 | `hecaton-spring-tests/pom.xml`, `hecaton-it/distribution-it/pom.xml`, `distribution/pom.xml`, `hecaton-parent/pom.xml`; deleted `TestHibernateApplicationContext`, `HibernateIT`, `AnnotatedEntity` and their resources | Removed the external `hazelcast-hibernate53` and `hazelcast-wm` integrations, which are built against `com.hazelcast` and cannot work with Hecaton. | M1 |
| 4 | `pom.xml`, `hecaton-parent/pom.xml` | Project metadata (URL, SCM, issue tracker, developers, organization) points to Hecaton instead of Hazelcast, Inc.; the Hazelcast contributor-agreement link is removed (Hecaton uses the DCO). | M1 |
| 5 | `checkstyle/checkstyle.xml` | `LineLength` max 130 → 132: `uk.org.hecaton` is one character longer than `com.hazelcast`, which pushed four upstream lines over the limit. Avoids patching individual files on every upstream release. | M1 |
| 6 | `hecaton-tpc-engine/src/main/resources/lib/linux-x86_64/libaffinity_helper.so`, `.../linux-x86_64/libicmp_helper.so`, `.../linux-x86/libicmp_helper.so` | Rebuilt from the renamed C sources with `tools/native/build-native.sh`. The upstream binaries export `Java_com_hazelcast_*` symbols and threw `UnsatisfiedLinkError` after the rename (thread affinity, ICMP ping). `tools/ci/check-native.sh` keeps binaries and sources in sync. | M1 |
| 7 | `hecaton/src/test/resources/1.serialization.compatibility.binary`, `testHz3Object`, `testHz3ObjectLittleEndian` | Regenerated with `tools/fixtures/regenerate-serialization-fixtures.sh` (upstream's `BinaryCompatibilityFileGenerator` and the `CompatibilitySerializationServiceTest` object). The serialized streams embed class names with length prefixes, so they cannot be renamed in place. Deterministic; byte layout otherwise unchanged. | M1 |
| 8 | deleted `hecaton/src/test/resources/uk/org/hecaton/nio/hz-3.1.5-multicast-package.dump` | Unused upstream fixture (no test reads it) that embeds Hazelcast 3 class names. | M1 |
| 9 | `XmlClientConfigBuilderTest`, `YamlClientConfigBuilderTest`, `WildcardConfigPatternMatcherTest`, `MatchingPointConfigPatternMatcherTest`, `AddressUtilTest`, `JobUploadClientSuccessTest`, `ReflectionUtilsTest` (hecaton), `SqlMappingTest` (hecaton-sql) | Test data the rename made inconsistent (one side of a pair renamed, lengths, checksums), found by the first nightly run: base64 token, wildcard patterns, a protected domain's pattern, SHA-256 of a rewritten jar, a fixed-size class-file buffer, an SQL error column. | M1 |
| 10 | `extensions/python/src/main/resources/jet_to_python_pb2.py` | The protoc-generated module embeds the serialized file descriptor, whose `java_package` option is a length-prefixed string. The rename made it one byte longer (`com.hazelcast` → `uk.org.hecaton`), so Python failed to load it ("Couldn't parse file content!") and every Python service job failed to start. Fixed the two length bytes (options message 59 → 60, package string 34 → 35). | M1 |
| 11 | `hecaton/src/test/java/uk/org/hecaton/map/EvictionMaxSizePolicyTest.java` | Upstream test bug, not rename-related: `testOwnerAndBackupEntryCountsAreEqualAfterEviction_whenPerNodeMaxSizePolicyIsUsed` compared `LocalMapStats` snapshots taken once right after the puts, so its "eventually" assertion retried on stale values and failed whenever backups or their eviction lagged (2 of 3 nightly runs on loaded CI runners). It now takes fresh stats on every attempt. Worth offering upstream. | M1 |
| 12 | `extensions/kafka-connect/pom.xml`; deleted `KafkaConnectSQSIT` | The test connectors were downloaded from `repository.hazelcast.com/download/tests/`. Six now come from their publishers (Confluent Hub, Maven Central), byte-identical to Hazelcast's copies (SHA-256 checked). The SQS connector has no public release and is an AWS connector (D9), so its test, download and AWS test dependencies are removed. | M1 |
| 13 | `HttpCommandParser`, `RestApiFilter` (one line each); new `RestPathAliases` | REST requests to `/hazelcast/...` are served as `/hecaton/...` (spec §5.4). The endpoint-group filter and the command parser both map the path, so the alias gets the same access checks. | M1 |
| 14 | `checkstyle/ClassHeaderApache.txt` | The header check also accepts the Hecaton header ("Copyright (c) <year>, The Hecaton Authors."), for Hecaton-owned files in upstream modules. | M1 |
| 15 | deleted `internal/util/phonehome` (and its `META-INF/services` file), the client endpoint statistics classes and their tests; `Node`, `NodeEngine(Impl)`, `ClientEngine(Impl)`, `NoOpClientEngine`, `ClientEndpointImpl`, `JetServiceBackend`, `ExecutionPlanBuilder`, `ClusterProperty`, `hecaton-spring-5.7.xsd` | Phone home reported usage to Hazelcast, Inc. and probed AWS, Azure and GCP metadata endpoints at startup. Hecaton has no reporting service, so the whole subsystem and the statistics collected only for it are gone, along with the `hecaton.phone.home.enabled` property and the `-D...=false` flags in the build. The SQL `skipUpdateStatistics` client message field stays (wire protocol) | M1 |
| 16 | `Protocols`, `UnifiedProtocolDecoder`, `SingleProtocolDecoder` | The member protocol bytes were the same as Hazelcast's and both report version 5.7, so a Hazelcast 5.7 member joined a Hecaton cluster (tried with a real member). The member protocol becomes `HCC`; connections that start with `HZC` are closed with a message saying why (spec NG1) | M1 |
| 17 | deleted `spring/hibernate/RegionFactoryBeanDefinitionParser`; `HecatonNamespaceHandler` | Served the Hibernate integration, which Hecaton does not have; `<hz:hibernate-region-factory>` fails with a message | M1 |
| 18 | `internal/json/JsonParser` | Arrays and objects are read with an explicit stack instead of recursion: on JDK 25 deep nesting could overflow the stack before the 1,000-level limit | M1 |
| 19 | `distribution/src/root/licenses/hazelcast-community-license.txt`, `distribution/src/root/LICENSE`, `distribution/src/main/resources/notice.txt` | The fork's rename had changed "Hazelcast" to "Hecaton" inside the distribution's copy of the Hazelcast Community License (and renamed the file), in the opt-folder LICENSE note and in the distribution NOTICE. Third-party license and notice texts are restored verbatim; the NOTICE now is Hecaton's followed by the original. The rename table keeps these paths unchanged | M1 |
| 20 | `bin/hc-start`, `HzStartIT` | Called `bin/hz`, which the rename had turned into `bin/hc` | M1 |
| 21 | `BuildInfo`, `BuildInfoProvider`, `GeneratedBuildProperties`, `DefaultNodeExtension`, logging services | Product version (D11, `0.1.0`) separate from the codebase version (`5.7.0`) that members and clients negotiate with; startup line "Hecaton Platform 0.1.0 (…, based on Hazelcast 5.7.0, cluster version 5.7)"; log prefixes show the product version; the copyright line credits both projects | M1 |
