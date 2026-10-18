#!/usr/bin/env bash
#
# Verifies that every committed JNI helper library exports exactly the JNI functions
# declared in its C source, so a package rename cannot leave stale binaries behind.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
failed=0
for lib in hecaton-tpc-engine/src/main/resources/lib/*/lib*.so; do
    name="$(basename "$lib" .so)"
    name="${name#lib}"
    expected="$(grep -oE 'Java_[A-Za-z0-9_]+' "hecaton/src/main/resources/$name.c" | sort -u)"
    actual="$(nm -D --defined-only "$lib" | grep -oE 'Java_[A-Za-z0-9_]+' | sort -u)"
    if [ "$expected" != "$actual" ]; then
        echo "FAIL: $lib exports"$'\n'"$actual"$'\n'"but $name.c declares"$'\n'"$expected"
        failed=1
    fi
done
[ "$failed" -eq 0 ] && echo "Native libraries match their JNI sources."
exit "$failed"
