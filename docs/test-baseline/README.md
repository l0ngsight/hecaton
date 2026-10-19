# Upstream test baselines

`upstream-<tag>-jdk25.txt` lists the tests that fail in **unmodified Hazelcast CE** at
`<tag>`, built and run on JDK 25 with the same nightly job layout Hecaton uses. The nightly
report does not count these as Hecaton regressions.

Test names are already mapped to Hecaton names (`com.hazelcast` → `uk.org.hecaton`,
`Hazelcast` → `Hecaton`), one `Class#method` per line.

To refresh a baseline (for example after a new upstream release is imported):

1. Run the **Nightly tests** workflow manually with `suite = upstream` and `ref` set to the
   branch whose `tools/upstream/version` names the tag.
2. Take `failures.txt` from the run's `test-results-upstream` artifact (or the block
   between `BEGIN/END FAILING TESTS` in the report job's log).
3. Commit it here as `upstream-<tag>-jdk25.txt`.
