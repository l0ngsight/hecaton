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

RESULTS
