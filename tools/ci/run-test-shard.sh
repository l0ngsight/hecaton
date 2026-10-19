#!/usr/bin/env bash
#
# Runs one shard of the nightly test suite (.github/workflows/nightly.yml).
#
# Usage: run-test-shard.sh <source-root> <hecaton|upstream> <group> <shard> <shards>
#
# Builds the group's modules and their dependencies (tests skipped), then runs the
# group's tests with upstream's standard `parallelTest` profile (SlowTest and
# NightlyTest excluded). Test failures do not fail the script; the report job
# evaluates the surefire XML.
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

./mvnw -B -ntp -pl "$modules" -PparallelTest test "${skip[@]}" "${test_args[@]}" \
    -Dmaven.test.failure.ignore=true -DtrimStackTrace=true || {
    echo "::warning::Maven exited with an error in $group shard $shard (see log); reports collected anyway."
}
