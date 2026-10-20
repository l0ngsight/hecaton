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
add Hecaton's auto-configuration for your Spring Boot version, `uk.org.hecaton:hecaton-spring-boot4`
(or `hecaton-spring-boot3`). It creates the `HecatonInstance` bean the same way: from a
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

Hecaton members cannot join a Hazelcast cluster, and persistence files are not compatible.
Stand up a Hecaton cluster and copy the data with a client-based copier or a Jet job, then
move the clients over. Official Hazelcast clients can connect to Hecaton (spec §5.4), so
clients can move before or after the application code. A `hecaton-migrate data` command is
planned (plan §1.5).

## What is checked

`migration-tests/` holds sample Hazelcast applications and the reviewed result of migrating
each one. CI runs each sample on Hazelcast 5.7, migrates it with both tools, requires the
result to equal the reviewed one, and runs it on Hecaton
([migration-tests/run.sh](../migration-tests/run.sh)).
