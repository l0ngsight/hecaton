# Vector collections (F20)

A vector collection stores documents (a value plus one or more vectors) under keys, and finds the
documents whose vectors are most similar to a query vector. The API (`uk.org.hecaton.vector`) and
the configuration (`VectorCollectionConfig`, `VectorIndexConfig`) are the ones Hazelcast 5.5+
has; the engine is Hecaton's, in the member module `uk.org.hecaton:hecaton-vector` (part of
`hecaton-member`). Without that module on a member's classpath, getting a vector collection fails
with "Vector collections (F20) requires uk.org.hecaton:hecaton-vector on the member's classpath."

```java
Config config = new Config();
config.addVectorCollectionConfig(new VectorCollectionConfig("articles")
        .addVectorIndexConfig(new VectorIndexConfig("text", Metric.COSINE, 384)));
HecatonInstance member = Hecaton.newHecatonInstance(config);

VectorCollection<String, Article> articles = VectorCollection.getCollection(member, "articles");
articles.setAsync("a1", VectorDocument.of(article, VectorValues.of("text", embedding)));
SearchResults<String, Article> similar = articles.searchAsync(VectorValues.of("text", query),
        SearchOptions.builder().limit(10).includeValue().build()).toCompletableFuture().join();
```

Clients use the same API (`VectorCollection.getCollection(client, "articles")`).

## What is supported

- **Configuration**: a collection needs a configuration with at least one index (static, or
  added at runtime with `VectorCollection.getCollection(instance, config)` or
  `Config#addVectorCollectionConfig`; wildcard names work). Per index: name (may be omitted
  when the collection has one index), metric, dimension, `max-degree`, `ef-construction`. Per
  collection: `backup-count`, `async-backup-count`, split-brain protection, merge policy,
  user code namespace.
- **Metrics and scores** (higher is more similar):

  | Metric | Score |
  |---|---|
  | `EUCLIDEAN` | `1 / (1 + d²)`, `d` the Euclidean distance |
  | `COSINE` | `(1 + cos) / 2` |
  | `DOT` | `(1 + dot) / 2` (meaningful for normalized vectors) |

- **Operations**: `get`, `put`, `set`, `putIfAbsent`, `putAll`, `remove`, `delete`, `clear`,
  `size`, `optimize` (rebuilds the indexes without the vectors removed since), `searchAsync`
  with `limit`, `includeValue`, `includeVectors` and the hint `efSearch` (how many candidates a
  search explores in each partition; default: the larger of `limit` and the index's
  `ef-construction`). A search takes one vector, for one index.
- **Documents** must have a vector of the index's dimension for every index; otherwise the write
  fails with `IllegalArgumentException`.
- **Backups and migrations**: the documents are kept in a partitioned IMap
  (`__vector.<collection>`) with the collection's backup counts; every replica indexes what it
  holds, so a backup that becomes an owner (after a member leaves) searches straight away.
- **Permissions**: `VectorCollectionPermission` with the actions `create`, `destroy`, `put`,
  `remove`, `read`, `optimize`, `all`.

## How it works

Each member keeps, for every partition replica it holds and every index, a Hierarchical Navigable
Small World (HNSW) graph (Malkov and Yashunin), written for Hecaton. Writes to the backing map,
whether from operations, backups or migrations, update the graph on the partition thread; a
removed vector stays in the graph as a waypoint until there are more removed than live vectors
(and at least 1024), or until `optimize`, when the graph is rebuilt. A partition holding no more
vectors than the search's candidate list is searched exactly.

A search goes to every data member, which searches the partitions it owns and returns its best
`limit` results; the caller merges them.

## Limits

- Searches are approximate (as in Hazelcast): HNSW can miss a near neighbour. Raise `efSearch`
  (or `ef-construction`, `max-degree`) for better recall at the cost of speed.
- While partitions migrate, a search can miss the documents of a migrating partition.
- Indexes are on the Java heap; the `useDeduplication` setting is accepted but identical vectors
  are not shared.
- No statistics or metrics for vector collections yet.
- Persistence (F10) does not cover vector collections; their documents are not persisted.
