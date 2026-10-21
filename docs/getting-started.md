# Getting started

Hecaton members run on **Java 25**; client applications on **Java 17** or later.

## Start a member

**Container image:**

```sh
docker run -p 5701:5701 ghcr.io/l0ngsight/hecaton:<version>
```

**Distribution:** download `hecaton-<version>.tar.gz` (or the `-slim` one, without the Jet
connectors and extensions) from the [GitHub releases](https://github.com/l0ngsight/hecaton/releases),
then

```sh
tar xzf hecaton-<version>.tar.gz
cd hecaton-<version>
bin/hc start                       # config/hecaton.xml; Ctrl-C to stop
```

Start more members the same way, on other machines or in other terminals: on one network they
find each other by multicast. `bin/hc-cli` is a SQL shell; `bin/hc-cluster-admin` changes the
cluster state and starts persistence operations.

**Embedded in an application:** depend on `uk.org.hecaton:hecaton-member`, which brings core,
SQL and the member modules (High-Density memory, persistence), and start a member in the JVM:

```xml
<dependency>
    <groupId>uk.org.hecaton</groupId>
    <artifactId>hecaton-member</artifactId>
    <version>${hecaton.version}</version>
    <type>pom</type>
</dependency>
```

```java
HecatonInstance member = Hecaton.newHecatonInstance();
```

Run it with `--enable-native-access=ALL-UNNAMED` (High-Density memory uses the FFM API). Use
`uk.org.hecaton:hecaton` alone for a member with core only.

## Connect a client

```xml
<dependency>
    <groupId>uk.org.hecaton</groupId>
    <artifactId>hecaton-client</artifactId>
    <version>${hecaton.version}</version>
    <type>pom</type>
</dependency>
```

```java
ClientConfig config = new ClientConfig();
config.getNetworkConfig().addAddress("127.0.0.1:5701");
HecatonInstance client = HecatonClient.newHecatonClient(config);

IMap<String, String> capitals = client.getMap("capitals");
capitals.put("uk", "London");
System.out.println(capitals.get("uk"));

client.getSql().execute("CREATE MAPPING capitals TYPE IMap OPTIONS ("
        + "'keyFormat'='varchar', 'valueFormat'='varchar')");
client.getSql().execute("SELECT * FROM capitals").forEach(System.out::println);
client.shutdown();
```

The official Hazelcast clients (Java 5.5–5.7, Python, Node.js, Go, .NET) connect to Hecaton
members too: point them at a member and use the cluster name (`dev` by default).

## Configure

Members read `hecaton.xml` or `hecaton.yaml` (from the working directory, the class path, or the
file named by `-Dhecaton.config`). Every setting can also be given as an environment variable:
`HC_` and the path in upper case, for example `HC_CLUSTERNAME=prod` or
`HC_NETWORK_JOIN_MULTICAST_ENABLED=false`. `hecaton-full-example.yaml` in the distribution
documents every setting.

```yaml
hecaton:
  cluster-name: prod
  network:
    join:
      multicast:
        enabled: false
      tcp-ip:
        enabled: true
        member-list: [10.0.0.1, 10.0.0.2, 10.0.0.3]
  map:
    orders:
      backup-count: 1
```

## Next

- The features Hecaton adds to Hazelcast CE: [TLS and security](security.md),
  [CP Subsystem](cp.md), [High-Density memory](hd.md), [persistence](persistence.md),
  [WAN replication](wan.md).
- [Running Hecaton in production](operations.md).
- [Migrating from Hazelcast](migration.md).
