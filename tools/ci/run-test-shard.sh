#!/usr/bin/env bash
#
# Runs one shard of the nightly test suite (.github/workflows/nightly.yml).
#
# Usage: run-test-shard.sh <source-root> <hecaton|upstream> <group> <shard> <shards>
#
# Builds the group's modules and their dependencies (tests skipped), then runs the
# group's tests with upstream's standard `parallelTest` profile (SlowTest and
# NightlyTest excluded). Test failures do not fail the script; the report job
# evaluates the surefire XML (gathered by collect-test-reports.sh).
set -euo pipefail

root="$1" suite="$2" group="$3" shard="$4" shards="$5"
tools="$(cd "$(dirname "$0")" && pwd)"
modules="$(python3 "$tools/nightly.py" modules --suite "$suite" --group "$group")"
skip=(-Dcheckstyle.skip -Dspotbugs.skip -Djacoco.skip -Dmaven.javadoc.skip -Denforcer.skip -Dlicense.skip)

cd "$root"
java -version

./mvnw -B -ntp -T 1C -pl "$modules" -am install -DskipTests "${skip[@]}"

test_args=()
if [ "$shards" -gt 1 ]; then
    tests="$(python3 "$tools/nightly.py" shard --root . --module "$modules" --shard "$shard" --shards "$shards")"
    echo "Shard $shard/$shards: $(tr ',' '\n' <<<"$tests" | wc -l) test classes"
    test_args=(-Dtest="$tests" -Dsurefire.failIfNoSpecifiedTests=false)
fi

# The parallelTest profile runs two surefire executions: default-test (ParallelJVMTest
# classes, forked in parallel) and singlejvm (everything else). Both write
# target/surefire-reports/TEST-<class>.xml, so for a class with tests of both kinds the
# second execution's report replaces the first one's and its failures go unreported.
# Run them as two passes and keep the first pass's reports in surefire-reports-paralleljvm.
#
# surefire.timeout kills a test JVM that runs longer than an hour (a hanging test), so the
# shard still finishes and reports what ran instead of hitting the job timeout.
mvn_test=(-B -ntp -pl "$modules" -PparallelTest "${skip[@]}" "${test_args[@]}"
    -Dmaven.test.failure.ignore=true -DtrimStackTrace=true -Dsurefire.timeout=3600)
warn() {
    echo "::warning::Maven exited with an error in $group shard $shard ($1 pass, see log); reports collected anyway."
}

./mvnw "${mvn_test[@]}" test -Dskip-singlejvm-tests=true || warn paralleljvm
find . -path '*/target/surefire-reports' -type d -prune -exec mv {} {}-paralleljvm \;
./mvnw "${mvn_test[@]}" surefire:test@singlejvm || warn singlejvm
