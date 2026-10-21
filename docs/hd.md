# High-Density memory

High-Density memory (F11) keeps the data of IMaps, ICaches and member Near Caches with the
`NATIVE` in-memory format outside the Java heap, so large data sets do not lengthen garbage
collection pauses. It is provided by the `uk.org.hecaton:hecaton-hd` module (Java 25), which
uses the Foreign Function and Memory API; `hecaton-member` and the distribution include it.

## Configuration

```yaml
hecaton:
  native-memory:
    enabled: true
    allocator-type: POOLED        # or STANDARD
    capacity:
      unit: GIGABYTES
      value: 32
    min-block-size: 16            # POOLED only
    page-size: 4194304            # POOLED only
    metadata-space-percentage: 12.5
  map:
    orders:
      in-memory-format: NATIVE
      eviction:
        eviction-policy: LRU
        max-size-policy: USED_NATIVE_MEMORY_PERCENTAGE
        size: 80
```

Members using native memory must run with `--enable-native-access=ALL-UNNAMED`; `hc start`
and the container image add it.

| Allocator | How it allocates |
|---|---|
| `STANDARD` | Every block from the C library's `malloc`. |
| `POOLED` | A buddy allocator over pages of `page-size` bytes (blocks are powers of two from `min-block-size` to the page size); each thread allocates from its own pool, empty pages go back to the operating system. Larger blocks come from `malloc`. |

The `capacity` bounds all native memory of the member. `metadata-space-percentage` of it is
reserved for metadata (hash tables, allocator bookkeeping), the rest is for keys, values and
records. When an allocation does not fit, the operation fails with `NativeOutOfMemoryError`; for
maps with an eviction policy, entries are evicted and the operation retried first.

With `POOLED`, every partition thread allocates from pages of its own, and eviction can only free
entries of the thread's own partitions. Keep the capacity at many pages per partition thread
(a member logs a warning below 8); lower `page-size` for small capacities.

Memory statistics (`MemoryStats`, the `memory.*Native*` metrics) report the capacity, the
committed and used native memory, and the metadata.

## What is stored natively

| Structure | Native | On the heap |
|---|---|---|
| IMap (`NATIVE`) | keys, values, record metadata (version, times, hits), the per-partition hash tables | expiry metadata of entries with a TTL or max idle; index entries (see below) |
| ICache (`NATIVE`) | keys, values, expiry policies, record metadata, hash tables | — |
| Member Near Cache (`NATIVE`) | values | records and keys |

Keys and values are handed to callers as heap copies, so no reference to native memory escapes
the storage.

## Max-size policies

`NATIVE` maps and caches support the native memory max-size policies:

| Policy | Evicts when |
|---|---|
| `USED_NATIVE_MEMORY_SIZE` | the structure uses more than `size` MB of native memory on the member |
| `USED_NATIVE_MEMORY_PERCENTAGE` | the structure uses more than `size` % of the member's native capacity |
| `FREE_NATIVE_MEMORY_SIZE` | the member has less than `size` MB of native memory free |
| `FREE_NATIVE_MEMORY_PERCENTAGE` | the member has less than `size` % of its native capacity free |

Maps also support `PER_NODE` and `PER_PARTITION`; Near Caches `ENTRY_COUNT`.

## Limitations

- Indexes of `NATIVE` maps are kept on the heap, with the indexed entries' values; off-heap
  index stores are planned. `BITMAP` indexes are not supported on `NATIVE` maps, as in Hazelcast.
- Member Near Cache records live on the heap and their values are freed when a record is
  replaced or garbage collected, so its native memory use can lag behind evictions.
- Client Near Caches with the `NATIVE` format (F11, P1) and persistent memory
  (`native-memory.persistent-memory`) are not available yet.

## Testing

`hecaton-hd`'s tests run on every CI build. The nightly `hd-native-suites` job runs core's map and
query test suites with `NATIVE` as the default map format (`-Pnative-suites`, system property
`hecaton.test.map.inMemoryFormat=NATIVE`).
