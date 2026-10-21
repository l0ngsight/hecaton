# Tiered Storage (F12)

A map with a tiered store keeps its values in native memory up to the capacity of its memory tier,
and the rest on disk, so it can hold more data than the members' memory. It is configured as in
Hazelcast (`TieredStoreConfig`, `MemoryTierConfig`, `DiskTierConfig`, `LocalDeviceConfig`) and
provided by the member module `uk.org.hecaton:hecaton-tiered` (part of `hecaton-member`; it uses
`hecaton-hd`'s native memory). Without the module, such a map is refused with "Tiered Storage (F12)
requires uk.org.hecaton:hecaton-tiered on the member's classpath."

```yaml
hecaton:
  native-memory:
    enabled: true
    capacity:
      unit: GIGABYTES
      value: 4
  local-device:
    disk:
      base-dir: /var/lib/hecaton/tiered
  map:
    big:
      in-memory-format: NATIVE
      tiered-store:
        enabled: true
        memory-tier:
          capacity:
            unit: MEGABYTES
            value: 512
        disk-tier:
          enabled: true
          device-name: disk
```

The rules of the configuration are Hazelcast's: native memory must be enabled, the map's in-memory
format must be `NATIVE`, the disk tier must be enabled and name a configured device, and the map
can have no eviction, TTL, max idle or data persistence.

## How it works

- Every partition of a tiered map has a record on the heap per entry (key, version, times) and
  keeps its values in two tiers: the **memory tier** (a native block per value) and the **disk
  tier** (an append-only file per partition under
  `<base-dir>/hecaton-tiered/<member UUID>/map-<hex name>/`).
- A written value goes to the memory tier. While the map's values in the memory tier of a member
  take more than `memory-tier.capacity`, the partition that writes moves its least recently used
  values to disk (each partition keeps at least the value it just wrote). A value read from disk
  by an operation moves back to the memory tier. When native memory runs out, values go straight
  to disk.
- Updated or removed values on disk become garbage; when a partition's file holds more garbage
  than live data (and at least 1 MB of it), the live values are copied to a new file.
- Backups and migrations work as for any map: the values are sent from wherever they are, and the
  receiving member stores them in its own tiers.
- The disk tier is a cache, not persistence: a member deletes its directory when it shuts down,
  and on start deletes the directories of members that are gone (unlocked directories).

## Limits

- The capacity of the memory tier is checked when a partition writes; partitions that do not
  write keep their values in memory, so the memory tier can stay above its capacity until they do.
- Keys and per-entry metadata stay on the Java heap.
- The device's `capacity`, `block-size` and I/O thread counts are accepted but not used.
- No statistics of the tiers yet beyond the owned entry memory cost (the memory tier's bytes).
