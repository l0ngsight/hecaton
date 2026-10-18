# Upstream patches

Every change Hecaton makes to a Hazelcast-derived file, beyond what `tools/rebrand`
generates, is listed here with its reason (spec §7.4). Keep the list short: each entry
is a potential merge conflict when a new upstream release comes in.

Generated changes are not listed. They are fully described by
[`tools/rebrand/rename-table.yaml`](../tools/rebrand/rename-table.yaml) and
[`tools/upstream/excluded-paths.txt`](../tools/upstream/excluded-paths.txt).

Budget: fewer than 40 patched upstream files (spec §7.4).

## Applied

| # | Files | Reason | Milestone |
|---|---|---|---|
| 1 | `hecaton-parent/pom.xml` | Removed Hazelcast's private Maven repositories (`repository.hazelcast.com` release/snapshot/security) and `distributionManagement`. Clean room (CONTRIBUTING §1); Hecaton publishes to Maven Central (M8). | M1 |
| 2 | `extensions/pom.xml`, `extensions/hadoop-dist/pom.xml`, `distribution/pom.xml`, `hecaton-coverage-report/pom.xml` | Dropped the cloud-provider modules left out at import (Kinesis, S3, files-azure/gcs/s3) and their dependencies (D9). | M1 |
| 3 | `hecaton-spring-tests/pom.xml`, `hecaton-it/distribution-it/pom.xml`, `distribution/pom.xml`, `hecaton-parent/pom.xml`; deleted `TestHibernateApplicationContext`, `HibernateIT`, `AnnotatedEntity` and their resources | Removed the external `hazelcast-hibernate53` and `hazelcast-wm` integrations, which are built against `com.hazelcast` and cannot work with Hecaton. | M1 |
| 4 | `pom.xml`, `hecaton-parent/pom.xml` | Project metadata (URL, SCM, issue tracker, developers, organization) points to Hecaton instead of Hazelcast, Inc.; the Hazelcast contributor-agreement link is removed (Hecaton uses the DCO). | M1 |
| 5 | `checkstyle/checkstyle.xml` | `LineLength` max 130 → 132: `uk.org.hecaton` is one character longer than `com.hazelcast`, which pushed four upstream lines over the limit. Avoids patching individual files on every upstream release. | M1 |

## Pending

Known follow-ups found while producing the renamed baseline. Each becomes an entry under
"Applied" when it lands.

| Item | Files (upstream-relative, renamed) | Reason |
|---|---|---|
| Cloud-provider removal (D9) | `config/AwsConfig`, `AzureConfig`, `GcpConfig`, `EurekaConfig`, `client/config/ClientCloudConfig`, `JoinConfig`, `ClientNetworkConfig`, `WanBatchPublisherConfig`, `AliasedDiscoveryConfigUtils`, config parsers/generators, XSD/JSON schemas, `META-INF/services/...DiscoveryStrategyFactory`, `HecatonCloudDiscovery`, `ViridianAddressProvider`, `extensions/pom.xml`, `extensions/hadoop-dist/pom.xml`, `distribution/pom.xml`, `hecaton-coverage-report/pom.xml` | Plan §1.6. The plugins themselves are left out at import. |
| Old-version compatibility tests | root `pom.xml` (`get-hz-3`), `hecaton/src/test/.../test/starter/**`, tests using `HecatonStarter` | They load real Hazelcast releases (`com.hazelcast` classes) next to Hecaton; cross-version compatibility with Hazelcast is a non-goal (NG1). Remove or disable. |
| Spring Hibernate support | `hecaton-spring/.../hibernate/RegionFactoryBeanDefinitionParser` | Spring XML support for the removed Hibernate integration. Decide whether to keep. |
| Kafka Connect test downloads | `extensions/kafka-connect/pom.xml` | Test connectors are downloaded from `repository.hazelcast.com/download/tests/`. Switch to the connectors' own distribution sites. |
| Binary test fixtures | 10 test jars (e.g. `client/console/testjob-*.jar`, `jet/core/DataSerializableSuppliers.jar`, `deployment/sample-pojo-1.0-car.jar`) and `*.serialization.compatibility.binary` | Contain compiled `com.hazelcast` class names that rebrand cannot rewrite. Rebuild from source or regenerate. |
| Native libraries | `hecaton-tpc-engine/src/main/resources/lib/linux-*/libaffinity_helper.so`, `libicmp_helper.so` | Prebuilt with JNI symbols `Java_com_hazelcast_*`; they no longer bind after the rename (thread affinity, ICMP ping). Rebuild from the renamed C sources. |
| Hazelcast 3 serialization fixtures | `nio/hz-3.1.5-multicast-package.dump`, `testHz3Object`, `testHz3ObjectLittleEndian` | Serialized Hazelcast 3 objects with `com.hazelcast` class names. Remove the tests or regenerate the fixtures. |
| REST `/hazelcast/...` alias | `internal/ascii/rest/HttpCommandProcessor` and REST dispatch | Spec §5.4: keep health checks and existing tooling working. |
| Spring Boot integration | `hecaton-spring-boot3/4` auto-configuration classes | They order themselves after Spring Boot's own Hazelcast auto-configuration (kept by name, protected in the rename table). Check that the ordering still makes sense for Hecaton. |
| JDK 25 build | plugin/library versions, Surefire `argLine` | Plan §1.2; only where JDK 25 needs it. |
