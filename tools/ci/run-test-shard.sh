#!/usr/bin/env bash
#
# Runs one shard of the nightly test suite (.github/workflows/nightly.yml).
#
# Usage: run-test-shard.sh <group> <shard> <shards>
#
# Builds the group's modules and their dependencies (tests skipped), then runs the
# group's tests with the `parallelTest` profile (SlowTest and
# NightlyTest excluded). Test failures do not fail the script; the report job
# evaluates the surefire XML.
set -euo pipefail

group="$1" shard="$2" shards="$3"
tools="$(cd "$(dirname "$0")" && pwd)"
modules="$(python3 "$tools/nightly.py" modules --group "$group")"
skip=(-Dcheckstyle.skip -Dspotbugs.skip -Djacoco.skip -Dmaven.javadoc.skip -Denforcer.skip -Dlicense.skip)

cd "$tools/../.."
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
# surefire.timeout asks a test JVM that runs longer than an hour (a hanging test) to stop,
# but that JVM still runs its shutdown hooks, and a member shutdown hook can hang as well.
# So each pass also has a hard limit: on expiry the hung test JVMs get a thread dump in the
# log and are killed, and the shard still reports what ran instead of hitting the job
# timeout.
mvn_test=(-B -ntp -pl "$modules" -PparallelTest "${skip[@]}" "${test_args[@]}"
    -Dmaven.test.failure.ignore=true -DtrimStackTrace=true -Dsurefire.timeout=3600)
pass_limit=70m
run_pass() {
    local name="$1"
    shift
    local status=0
    timeout --kill-after=60 "$pass_limit" ./mvnw "${mvn_test[@]}" "$@" || status=$?
    if [ "$status" -eq 124 ] || [ "$status" -eq 137 ]; then
        echo "::warning::$group shard $shard: $name pass exceeded $pass_limit; killing hung test JVMs."
        for pid in $(pgrep -f surefire/surefirebooter || true); do
            echo "----- thread dump of test JVM $pid -----"
            jcmd "$pid" Thread.print 2>&1 | head -n 600 || true
        done
        pkill -9 -f surefire/surefirebooter || true
    elif [ "$status" -ne 0 ]; then
        echo "::warning::Maven exited with an error in $group shard $shard ($name pass, see log); reports collected anyway."
    fi
}

run_pass paralleljvm test -Dskip-singlejvm-tests=true
find . -path '*/target/surefire-reports' -type d -prune -exec mv {} {}-paralleljvm \;
run_pass singlejvm surefire:test@singlejvm
