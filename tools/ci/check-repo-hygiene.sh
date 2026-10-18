#!/usr/bin/env bash
#
# Repository hygiene checks run in CI on every Hecaton branch.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
failed=0
fail() { echo "FAIL: $*"; failed=1; }

# 1. Licensing files.
grep -q "Apache License" LICENSE && grep -q "Version 2.0, January 2004" LICENSE \
    || fail "LICENSE must be the Apache License 2.0"
[ -s NOTICE ] || fail "NOTICE is missing"
grep -q "Hazelcast, Inc." NOTICE || fail "NOTICE must credit the original work by Hazelcast, Inc."

# 2. Clean room: no Hazelcast Enterprise artifacts or repositories in builds.
ee_refs="$(git grep -n -I -E 'hazelcast-enterprise|repository\.hazelcast\.com' \
    -- '*pom.xml' '*.gradle' '*.gradle.kts' '*settings.xml' || true)"
[ -z "$ee_refs" ] || fail "references to Hazelcast Enterprise artifacts/repositories:"$'\n'"$ee_refs"

# 3. Hecaton-owned code must not use sun.misc.Unsafe.
unsafe_refs="$(git grep -n -I 'sun\.misc\.Unsafe' -- 'hecaton-ext/**/*.java' 'tools/**/*.java' || true)"
[ -z "$unsafe_refs" ] || fail "sun.misc.Unsafe used in Hecaton-owned code:"$'\n'"$unsafe_refs"

if [ "$failed" -ne 0 ]; then
    exit 1
fi
echo "Repository hygiene checks passed."
