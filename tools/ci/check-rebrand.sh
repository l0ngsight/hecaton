#!/usr/bin/env bash
#
# Verifies that rebrand/<tag> is exactly what tools/rebrand produces from the
# upstream/main snapshot of <tag> with the current rename table.
#
# Usage: tools/ci/check-rebrand.sh [<tag>]     (default: the tag in tools/upstream/version)
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
tag="${1:-$(cat tools/upstream/version)}"
java_bin="${JAVA_HOME:+$JAVA_HOME/bin/}java"

for ref in upstream/main "rebrand/$tag"; do
    if ! git rev-parse --verify --quiet "$ref" >/dev/null; then
        git fetch --quiet origin "$ref:$ref"
    fi
done

snapshot="$(git log upstream/main --format=%H --grep="^Upstream-Tag: $tag$" -n 1)"
[ -n "$snapshot" ] || { echo "FAIL: no snapshot of $tag on upstream/main"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir "$work/source" "$work/target"
git archive "$snapshot" | tar -x -C "$work/source"

"$java_bin" tools/rebrand/Rebrand.java "$work/source" "$work/target" | grep -E '^- '

git -C "$work/target" init --quiet
git -C "$work/target" add --all --force
expected="$(git -C "$work/target" write-tree)"
actual="$(git rev-parse "rebrand/$tag^{tree}")"

if [ "$expected" != "$actual" ]; then
    echo "FAIL: rebrand/$tag does not match the output of tools/rebrand (tree $actual, expected $expected)."
    echo "Regenerate it with tools/rebrand/rebrand.sh $tag."
    exit 1
fi
echo "rebrand/$tag matches tools/rebrand output (tree $actual)."
