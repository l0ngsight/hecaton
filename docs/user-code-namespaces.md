# User Code Namespaces

User Code Namespaces (F17) let members run user code (map stores, entry processors, listeners,
executor tasks and the other customizations of a data structure) from JARs and classes that are
not on the members' classpath, with a separate set per namespace, and replace them while the
cluster runs. The configuration and API are Hazelcast's.

## Configuration

```yaml
hecaton:
  user-code-namespaces:
    enabled: true
    name-spaces:
      ns1:
        - id: "orders"
          resource-type: JAR            # JAR, JARS_IN_ZIP or CLASS
          url: "file:///opt/app/orders-1.4.jar"
  map:
    orders:
      user-code-namespace: ns1
      map-store:
        enabled: true
        class-name: com.example.OrdersStore
```

In Java, `UserCodeNamespaceConfig#addJar`, `addJarsInZip` and `addClass` add the resources;
`Config#getNamespacesConfig()` holds the namespaces. The data structures with a
`user-code-namespace` setting (maps, caches, queues, lists, sets, multimaps, replicated maps,
topics, ringbuffers, executors, ...) run their user code with the namespace's classes.

- A namespace's classes are looked up in the namespace first, then on the member's classpath.
  Classes with the same name in different namespaces are separate classes.
- The namespace named `default`, if configured, is used by the data structures that do not name
  a namespace.
- Adding a namespace at runtime (`getConfig().getNamespacesConfig().addNamespaceConfig(...)` on a
  member or a client) adds it to every member, also those that join later; adding one with the
  name of an existing namespace replaces it for the code that runs afterwards.
- The `class-filter` (a blacklist and whitelist of classes, packages and prefixes) restricts
  which classes the namespaces may define.
- Statistics (resources and their sizes per namespace) are in the metrics under `ucn` and
  `ucn.resource`.

## Limitations

- The resources are read when the namespace is added (from its URLs on the member that adds
  it) and kept in memory on every member.
- Replacing a namespace does not reload objects already created from its classes (a map's
  existing `MapStore` instance, for example); they keep the classes they were created with.

## Testing

`UserCodeNamespacesTest` compiles map loaders at test time, so their classes are not on the
classpath, and checks the namespace of a map, isolation between namespaces, the default
namespace, a namespace added at runtime on a two-member cluster, the class filter and the
statistics.
