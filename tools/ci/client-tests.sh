#!/usr/bin/env bash
#
# Client checks on the JDK selected by the CI matrix (17, 21, 25). Uses the jar built on
# JDK 25 by the build job (bytecode level 17). Members need Java 25 (spec §8.2), so the member
# runs in its own JVM on MEMBER_JAVA (default: $JAVA_HOME_25_X64/bin/java, else java); on an
# older JDK the script also checks that a member refuses to start.
#
# Usage: tools/ci/client-tests.sh [path/to/hecaton-VERSION.jar]
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
java -version

jar="${1:-$(ls hecaton/target/hecaton-*.jar | grep -vE -- '-(sources|tests|javadoc)\.jar$' | head -n 1)}"
member_java="${MEMBER_JAVA:-${JAVA_HOME_25_X64:+$JAVA_HOME_25_X64/bin/}java}"
port=5971

work="$(mktemp -d)"
member_pid=
cleanup() {
    if [ -n "$member_pid" ]; then
        kill "$member_pid" 2>/dev/null || true
        wait "$member_pid" 2>/dev/null || true
    fi
    rm -rf "$work"
}
trap cleanup EXIT

cat > "$work/member.yaml" <<YAML
hecaton:
  cluster-name: client-smoke
  network:
    port:
      port: $port
      auto-increment: false
    join:
      multicast:
        enabled: false
      auto-detection:
        enabled: false
YAML

"$member_java" -Dhecaton.config="$work/member.yaml" -cp "$jar" uk.org.hecaton.core.server.HecatonMemberStarter \
    > "$work/member.log" 2>&1 &
member_pid=$!

if ! java -cp "$jar" tools/ci/ClientSmoke.java "127.0.0.1:$port"; then
    cat "$work/member.log"
    exit 1
fi

feature="$(java -XshowSettings:properties -version 2>&1 | sed -En 's/^ *java.specification.version = ([0-9]+).*$/\1/p')"
if [ "$feature" -lt 25 ]; then
    if output="$(timeout 120 java -cp "$jar" uk.org.hecaton.core.server.HecatonMemberStarter 2>&1)"; then
        echo "A member started on Java $feature"
        exit 1
    fi
    if ! grep -qF "Hecaton members require Java 25+" <<<"$output"; then
        echo "$output"
        echo "A member on Java $feature did not fail with the expected message"
        exit 1
    fi
    echo "A member refuses to start on Java $feature, as expected"
fi
