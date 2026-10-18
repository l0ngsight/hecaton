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

# 2. Clean room: no Hazelcast Enterprise artifacts, and no Maven repositories hosted by
#    Hazelcast (they serve Enterprise and private builds).
ee_refs="$(git grep -n -I -E '<artifactId>hazelcast-enterprise' \
    -- '*pom.xml' '*.gradle' '*.gradle.kts' || true)"
[ -z "$ee_refs" ] || fail "references to Hazelcast Enterprise artifacts:"$'\n'"$ee_refs"
repo_refs="$(git ls-files -z -- '*pom.xml' '*settings.xml' | xargs -0 -r perl -0777 -ne \
    'while (/<((?:plugin|snapshot)?[Rr]epository)>(.*?)<\/\1>/gs) {
         my ($tag, $body) = ($1, $2);
         print "$ARGV: <$tag> " . ($body =~ s/\s+/ /gr) . "\n" if $body =~ m{repository\.hazelcast\.com};
     }' || true)"
[ -z "$repo_refs" ] || fail "Maven repositories hosted by Hazelcast:"$'\n'"$repo_refs"

# 3. Hecaton-owned code must not use sun.misc.Unsafe.
unsafe_refs="$(git grep -n -I 'sun\.misc\.Unsafe' -- 'hecaton-ext/**/*.java' 'tools/**/*.java' || true)"
[ -z "$unsafe_refs" ] || fail "sun.misc.Unsafe used in Hecaton-owned code:"$'\n'"$unsafe_refs"

if [ "$failed" -ne 0 ]; then
    exit 1
fi
echo "Repository hygiene checks passed."
