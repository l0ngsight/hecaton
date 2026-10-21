# Migrating from Hazelcast to Hecaton

Hecaton is Hazelcast CE 5.7 renamed (spec §5). Migrating an application is mostly a
mechanical rename, done by two tools:

| Tool | Migrates |
|---|---|
| OpenRewrite recipe `uk.org.hecaton.migrate.FromHazelcast` | Java code, Maven and Gradle builds |
| `hecaton-migrate` | Configuration (XML, YAML, `.properties`), `.env` files, Dockerfiles, Kubernetes manifests, Helm templates, scripts |

Read [changes-from-hazelcast.md](changes-from-hazelcast.md) first: it lists what Hecaton
does not have (cloud-provider discovery, some connectors) and what changed beyond names.

## 1. Java code and build

From the project directory, with the Hazelcast dependencies still in place (the recipe
needs them to see which Hazelcast types the code uses):

```sh
mvn org.openrewrite.maven:rewrite-maven-plugin:6.46.1:run \
  -Drewrite.recipeArtifactCoordinates=uk.org.hecaton:hecaton-migrate-rewrite:<hecaton-version> \
  -Drewrite.activeRecipes=uk.org.hecaton.migrate.FromHazelcast
```

The recipe:

- moves `com.hazelcast.*` to `uk.org.hecaton.*` and renames "Hazelcast" to "Hecaton" in the
  types, methods and constants the code uses: `HazelcastInstance` → `HecatonInstance`,
  `Hazelcast.newHazelcastInstance()` → `Hecaton.newHecatonInstance()`, and methods the code
  overrides, such as `setHazelcastInstance` → `setHecatonInstance`;
- renames property keys, config file names, class names, `HZ_` variables and the SQL
  catalog (`hazelcast.public.x` → `hecaton.public.x`) in string literals;
- replaces `com.hazelcast:hazelcast*` and `com.hazelcast.jet:hazelcast-jet-*` dependencies
  (also in `<dependencyManagement>`) with the Hecaton artifacts.

Your own names are left alone: a variable called `hazelcastInstance` or a map called
`"hazelcast-sessions"` keeps its name. Comments are not changed.

**Spring Boot:** Spring Boot's built-in Hazelcast support only recognises `com.hazelcast`, so
the recipe adds Hecaton's auto-configuration for your Spring Boot version,
`uk.org.hecaton:hecaton-spring-boot4` or `hecaton-spring-boot3` (Maven builds; add it yourself
with Gradle). It creates the `HecatonInstance` bean the same way: from a
`Config` or `ClientConfig` bean, from `spring.hecaton.config` (renamed from
`spring.hazelcast.config` by `hecaton-migrate`), or from a default `hecaton.xml`/`.yaml` or
`hecaton-client.xml`/`.yaml`.

## 2. Configuration and other files

```sh
java -jar hecaton-migrate-<hecaton-version>.jar .            # show what would change
java -jar hecaton-migrate-<hecaton-version>.jar --apply .    # make the changes
```

It renames:

- the default config files (`hazelcast.xml` → `hecaton.xml`, `hazelcast-client.yaml` →
  `hecaton-client.yaml`, ...), their root elements and keys, the schema namespace and XSDs
  (`https://hecaton.org.uk/schema/config`), `<hz:hazelcast>` in Spring XML;
- property keys and file names that Hecaton reads (`hazelcast.logging.type` →
  `hecaton.logging.type`), `com.hazelcast` class and logger names, the JMX domain;
- `HZ_` / `HZCLIENT_` environment variables, `-Dhz.` overrides, `hz-*` scripts and
  `spring.hazelcast.config`.

Lines that still mention Hazelcast are listed as `review` lines. They are your own names
or things with no Hecaton equivalent, such as a `hazelcast/hazelcast` container image: check
each one. Java sources and build files are skipped (the recipe handles them). `--check`
exits with status 1 if anything would change, for use in CI.

## 3. Data

Hecaton members cannot join a Hazelcast cluster (they refuse each other's connections), and
persistence files are not compatible. Stand up a Hecaton cluster and copy the data:

```sh
java -jar hecaton-migrate-<hecaton-version>-cli.jar data \
    --source hz1:5701,hz2:5701 --source-cluster prod \
    --target hc1:5701 --target-cluster prod
```

It copies every IMap, MultiMap and ReplicatedMap (`--name` selects some) in serialized form,
so it needs none of your classes; Compact data keeps its schemas. `--source` and `--target`
also accept a client configuration file, for TLS or authentication. Stop writes to the source
first for an exact copy. ICache contents are not copied yet.

Official Hazelcast clients can connect to Hecaton (spec §5.4), so clients can move before or
after the application code.

## 4. From Hazelcast Enterprise

Hecaton provides open-source implementations of most Enterprise features, configured the same
way: after the steps above an Enterprise configuration works unchanged where this table says
so. Features that are not available yet fail at startup with a message naming the milestone
that delivers them, rather than being ignored.

| Enterprise feature | In Hecaton | Notes |
|---|---|---|
| License key | Not needed | `license-key` is ignored with an INFO message |
| TLS/SSL (member, client, WAN, REST, Memcache) | Yes (F1) | `BasicSSLContextFactory` and custom factories; OpenSSL factory not yet. [security.md](security.md), [ee-class-map.md](ee-class-map.md) |
| Security: realms, JAAS, member authentication, client permissions, security interceptors | Yes (F2, F3) | LDAP and Kerberos realms. [security.md](security.md) |
| Audit log, socket interceptor | Yes (F4, F5) | [security.md](security.md) |
| Symmetric encryption | No (F6) | Deprecated in Hazelcast; use TLS |
| CP Subsystem, CPMap | Yes (F7, F8, F9) | Including CP persistence; files are not compatible with Hazelcast's. [cp.md](cp.md) |
| High-Density memory (`NATIVE` format) | Yes (F11) | IMap, ICache, member and client Near Caches (clients need `hecaton-hd` and Java 25); indexes of `NATIVE` maps are on the heap. [hd.md](hd.md) |
| Persistence (Hot Restart) | Yes (F10) | IMap, ICache, cluster metadata, coordinated restart, partial/force start, hot backup; encryption at rest (Java KeyStore or Vault master keys, key rotation); files are not compatible with Hazelcast's. [persistence.md](persistence.md) |
| WAN replication | Yes (F13) | Batch publisher, consumer, management, full and Merkle tree sync, consistency check; replicates to Hecaton clusters only. [wan.md](wan.md) |
| Client blue/green failover | Yes (F15) | |
| Rolling upgrades | 1.x (F14) | |
| Dynamic configuration persistence and reload | Yes (F16) | [dynamic-configuration.md](dynamic-configuration.md) |
| User Code Namespaces | Yes (F17) | [user-code-namespaces.md](user-code-namespaces.md) |
| Tiered Storage, Jet lossless restart and job upgrades, SQL catalog persistence, vector collections | 2.x (F12, F18, F19, F20) | |
| Management Center | No | Out of scope; the operations are available through the API, REST and `hc-cluster-admin` |

[changes-from-hazelcast.md](changes-from-hazelcast.md) lists every difference applications
can observe.

## What is checked

`migration-tests/` holds sample Hazelcast applications and the reviewed result of migrating
each one. CI runs each sample on Hazelcast 5.7, migrates it with both tools, requires the
result to equal the reviewed one, and runs it on Hecaton
([migration-tests/run.sh](../migration-tests/run.sh)).
