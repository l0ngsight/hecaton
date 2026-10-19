# Upstream patches

Every change Hecaton makes to a Hazelcast-derived file, beyond what `tools/rebrand`
generates, is listed here with its reason (spec §7.4). Keep the list short: each entry
is a potential merge conflict when a new upstream release comes in.

Generated changes are not listed. They are fully described by
[`tools/rebrand/rename-table.yaml`](../tools/rebrand/rename-table.yaml) and
[`tools/upstream/excluded-paths.txt`](../tools/upstream/excluded-paths.txt). That includes
the eight upstream test jars, which `tools/rebrand` rewrites (entry paths and class-file
constant pools), and the `@Generated` integrity hashes of the client-protocol codecs, which it
recomputes.

Budget: fewer than 40 patched upstream files (spec §7.4).

## Applied

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

## Pending

Known follow-ups found while producing the renamed baseline. Each becomes an entry under
"Applied" when it lands.

| Item | Files (upstream-relative, renamed) | Reason |
|---|---|---|
| Cloud-provider removal (D9) | `config/AwsConfig`, `AzureConfig`, `GcpConfig`, `EurekaConfig`, `client/config/ClientCloudConfig`, `JoinConfig`, `ClientNetworkConfig`, `WanBatchPublisherConfig`, `AliasedDiscoveryConfigUtils`, config parsers/generators, XSD/JSON schemas, `META-INF/services/...DiscoveryStrategyFactory`, `HecatonCloudDiscovery`, `ViridianAddressProvider`, `extensions/pom.xml`, `extensions/hadoop-dist/pom.xml`, `distribution/pom.xml`, `hecaton-coverage-report/pom.xml` | Plan §1.6. The plugins themselves are left out at import. |
| Old-version compatibility tests | root `pom.xml` (`get-hz-3`), `hecaton/src/test/.../test/starter/**`, tests using `HecatonStarter` | They load real Hazelcast releases (`com.hazelcast` classes) next to Hecaton; cross-version compatibility with Hazelcast is a non-goal (NG1). Remove or disable. |
| Spring Hibernate support | `hecaton-spring/.../hibernate/RegionFactoryBeanDefinitionParser` | Spring XML support for the removed Hibernate integration. Decide whether to keep. |
| Kafka Connect test downloads | `extensions/kafka-connect/pom.xml` | Test connectors are downloaded from `repository.hazelcast.com/download/tests/`. Switch to the connectors' own distribution sites. |
| REST `/hazelcast/...` alias | `internal/ascii/rest/HttpCommandProcessor` and REST dispatch | Spec §5.4: keep health checks and existing tooling working. |
| Spring Boot integration | `hecaton-spring-boot3/4` auto-configuration classes | They order themselves after Spring Boot's own Hazelcast auto-configuration (kept by name, protected in the rename table). Check that the ordering still makes sense for Hecaton. |
| JDK 25 build | plugin/library versions, Surefire `argLine` | Plan §1.2; only where JDK 25 needs it. |
