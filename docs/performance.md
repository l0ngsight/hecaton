# Performance

Spec §10 asks that Hecaton be no more than 5% slower than Hazelcast CE 5.7 on IMap get and put
with on-heap data, and that High-Density memory keep GC pauses short with large native data sets.

## Benchmarks against Hazelcast CE

[`benchmarks/`](../benchmarks) holds JMH benchmarks that run the same workload on Hecaton and on
Hazelcast CE 5.7: a two-member cluster in one JVM, an IMap of 100,000 entries, and 8 threads
calling `get`, `set` or `put` on random keys, either on a member or through a client, with
100-byte and 1 KB values. Every JMH fork runs one product in its own JVM.

```sh
./mvnw -B -ntp -DskipTests install                     # Hecaton into the local repository
mvn -f benchmarks/pom.xml package
java -jar benchmarks/target/benchmarks.jar -rf json -rff results.json
tools/perf/compare.py results.json                     # table; exit status 1 on a >5% regression
```

`java -jar benchmarks/target/benchmarks.jar -h` lists the JMH options, for example `-p caller=client`
to run only the client benchmarks. Run them on a quiet machine with at least 4 cores; shared CI
runners are too noisy to compare runs, so the benchmarks are not part of CI.

## Results

First run (Hecaton 0.8.0-test against Hazelcast CE 5.7.0, JDK 25, a shared 4-core VM, one fork,
3 × 5 s warm-up and 3 × 10 s measurement per case; throughput in operations per second, with the
99.9% confidence interval):

| Benchmark | Caller | Value | Hazelcast CE 5.7 | Hecaton | Hecaton / Hazelcast |
|---|---|---|---|---|---|
| get | member | 100 B | 61,932 ± 38,322 | 60,636 ± 16,220 | 0.98 |
| get | member | 1 KB | 62,379 ± 62,596 | 63,267 ± 31,314 | 1.01 |
| get | client | 100 B | 36,990 ± 25,790 | 36,082 ± 40,199 | 0.98 |
| get | client | 1 KB | 36,008 ± 52,751 | 35,111 ± 20,074 | 0.98 |
| set | member | 100 B | 32,327 ± 2,659 | 32,269 ± 25,668 | 1.00 |
| set | member | 1 KB | 28,895 ± 8,972 | 31,503 ± 15,978 | 1.09 |
| set | client | 100 B | 23,415 ± 16,530 | 23,107 ± 11,684 | 0.99 |
| set | client | 1 KB | 19,110 ± 53,362 | 22,191 ± 23,659 | 1.16 |
| put | member | 100 B | 32,621 ± 15,662 | 32,197 ± 34,678 | 0.99 |
| put | member | 1 KB | 30,556 ± 12,684 | 30,194 ± 8,530 | 0.99 |
| put | client | 100 B | 24,190 ± 9,272 | 22,830 ± 8,100 | 0.94 |
| put | client | 1 KB | 21,977 ± 11,922 | 20,428 ± 9,845 | 0.93 |

On this machine the differences are well inside the confidence intervals: the run does not show a
regression, nor rule out one of a few percent. Client `put` was measured again with two forks and
5 × 10 s (Hecaton: 22,301 ± 652 ops/s with 100 B values, 22,261 ± 966 with 1 KB); the Hazelcast half
of that run was not completed. The §10 gate needs a run on a dedicated machine with the default
settings (`java -jar benchmarks/target/benchmarks.jar`, two forks per case), which is open.

## Hot reads of a large value

Phase 0 of [load protection](design/load-protection.md): what do members allocate when many
readers read the same large value? `HotReadAllocation` (in `benchmarks/`) starts a two-member
cluster and its readers in one JVM, stores one value, reads it with many reads in flight and
records the reads with Flight Recorder. Fixed 64 KB TLABs make Flight Recorder record every
allocation above 64 KB with its stack, so the report counts exactly how many value-sized objects the
member threads allocated per read, and where; `ThreadMXBean` gives the exact bytes per member
thread. Each scenario runs in its own JVM, on Hecaton or on Hazelcast CE 5.7.

```sh
./mvnw -B -ntp -DskipTests -pl hecaton -am install    # Hecaton into the local repository
mvn -f benchmarks/pom.xml package
tools/perf/hot-read-allocation.sh target/fanout        # summary.md, and per scenario a .md and a .jfr
```

The script's scenarios cover clients connected to the owner (smart routing) or to another member,
the `BINARY` and `OBJECT` formats, TLS, embedded members reading through their own map proxies
(blocking or asynchronous, with and without a near cache), and a new client per read. Single runs
take options such as `--value-size 4m`, `--gets 5000`, `--concurrency 1024`, and
`--max-copies-per-read 0`, which makes the run fail (exit status 1) when the member threads copy
the value; the `.jfr` files open in JDK Mission Control.

### Results

First run: a 1 MB value, 2,000 reads with 256 in flight, a shared 4-core VM (full report with the
allocation stacks: [results/hot-read-allocation-2026-10-22.md](results/hot-read-allocation-2026-10-22.md)).
"Copies" are the value-sized objects allocated in the members per read. Hazelcast CE 5.7.0 and
Hecaton allocate the same on every path measured on both.

| Read path | Hazelcast 5.7: copies, member bytes per read | Hecaton: copies, member bytes per read |
|---|---|---|
| Client to the owner, `BINARY` | 0, 2 KB | 0, 1 KB |
| Client to the owner, `BINARY`, TLS | — (not in CE) | 0, 82 KB (JDK TLS cipher objects) |
| Client to another member, `BINARY` | 3, 3.0 MB | 3, 3.2 MB (TLS) |
| Client to the owner, `OBJECT` | 2, 2.1 MB | 2, 2.1 MB |
| Member reads a key of another member, `getAsync` | 5, 5.0 MB | 5, 5.2 MB (TLS) |
| Member reads a key of another member, `get` | 4, 4.0 MB | 4, 4.2 MB (TLS) |
| … with a member near cache, `BINARY` | 1, 1.0 MB | — |
| … with a member near cache, `OBJECT` | 0, 11 B | — |
| … the same, cold, 256 concurrent first reads | 0.75 (48 reads went to the owner), 0.75 MB | — |
| A new client per read, smart routing | 0, 0.66 MB per connection | 0, 1.7 MB per connection (TLS) |

The copies on the member-to-member path: the owner serialises the response into a packet
(`OutboundResponseHandler#toNormalResponsePacket`), the receiving member reads the packet
(`PacketIOHelper#readValue`), deserialises the response (`NormalResponse#readData`) — once per
stage waiting on the invocation's future (`InvocationFuture#resolve`), so three times for a
`getAsync` with a callback — and converts it to the caller's object (`MapProxySupport#toObject`).
Throughput on a shared VM is only indicative; the near cache `OBJECT` run served 8,675 reads/s
against 630 without it.

### 50,000 reads in flight

With 50,000 reads of a 1 MB value in flight, services reading through an embedded member run
Hazelcast 5.7 and Hecaton out of an 8 GB heap within half a minute; with load protection and an
immutable map they copy nothing and need no GC. Smart clients complete in every version. Details,
and a TCP stall that the run found and that is fixed:
[results/hot-read-50k-2026-10-22.md](results/hot-read-50k-2026-10-22.md). `--load-protection`
runs `HotReadAllocation` with load protection on and the map immutable.
