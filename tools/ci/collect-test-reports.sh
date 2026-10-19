#!/usr/bin/env bash
#
# Copies the surefire XML reports of a nightly shard into one directory for upload.
#
# Usage: collect-test-reports.sh <out-dir> <source-root>...
#
# Reports of the parallel-JVM pass (target/surefire-reports-paralleljvm, see
# run-test-shard.sh) get a "-paralleljvm" suffix so they do not replace the reports of
# the same class from the single-JVM pass.
set -euo pipefail

out="$1"
shift
mkdir -p "$out"
for root in "$@"; do
    [ -d "$root" ] || continue
    find "$root" -path '*/target/surefire-reports*/TEST-*.xml' -print0 |
        while IFS= read -r -d '' report; do
            suffix="$(basename "$(dirname "$report")")"
            suffix="${suffix#surefire-reports}"
            name="$(basename "$report" .xml)"
            cp "$report" "$out/$name$suffix.xml"
        done
done
echo "$(find "$out" -name 'TEST-*.xml' | wc -l) report files"
