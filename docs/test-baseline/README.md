# Inherited test failures

`hazelcast-5.7.0.txt` lists the tests that fail in **unmodified Hazelcast CE 5.7.0**,
built and run on JDK 25 with the nightly job layout, before Hecaton was forked (two runs,
see the file header). The nightly report counts them separately from Hecaton regressions.

Test names are mapped to Hecaton names (`com.hazelcast` → `uk.org.hecaton`, `Hazelcast` →
`Hecaton`), one `Class#method` per line.

The list is frozen: Hecaton does not track later Hazelcast releases (spec §9). Remove an
entry when the test is fixed; never add new ones (Hecaton-specific failures belong in
`tools/ci/known-test-failures.txt`, with a reason).
