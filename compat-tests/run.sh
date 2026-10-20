#!/usr/bin/env bash
#
# Official-client compatibility test (spec G3, plan §1.4).
#
# Starts a Hecaton member (hecaton + hecaton-sql jars from this build) and runs the official
# Hazelcast clients against it: Java 5.5/5.6/5.7, Python, Node.js, Go and .NET, each with
# IMap + entry listener, queue, topic, SQL and Compact scenarios. Then starts a Hazelcast 5.7
# member that tries to join and checks that it is refused (spec NG1).
#
# Usage: compat-tests/run.sh [SCENARIO...]
#   SCENARIO: java-5.5.0 java-5.6.0 java-5.7.0 python nodejs go dotnet hazelcast-member (default: all)
#   COMPAT_SKIP_MISSING=1 skips clients whose runtime (python3, node, go, dotnet) is not installed.
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
tests="$root/compat-tests"
mvn=("$root/mvnw" -B -ntp -q)

scenarios=("$@")
if [ ${#scenarios[@]} -eq 0 ]; then
    scenarios=(java-5.5.0 java-5.6.0 java-5.7.0 python nodejs go dotnet hazelcast-member)
fi

jar_of() {
    ls "$root/$1/target/$1"-*.jar | grep -vE -- '-(sources|tests|javadoc)\.jar$' | head -n 1
}
hecaton_jar="$(jar_of hecaton)"
sql_jar="$(jar_of hecaton-sql)"

work="$(mktemp -d)"
member_pid=""
cleanup() {
    if [ -n "$member_pid" ]; then
        kill "$member_pid" 2>/dev/null || true
        wait "$member_pid" 2>/dev/null || true
    fi
    rm -rf "$work"
}
trap cleanup EXIT

# Fetches a Hazelcast jar from Maven Central into $work and prints its path.
hazelcast_jar() {
    "${mvn[@]}" -f "$tests/java/pom.xml" dependency:copy -Dartifact="com.hazelcast:hazelcast:$1" \
        -DoutputDirectory="$work/jars" >&2
    echo "$work/jars/hazelcast-$1.jar"
}

has() {
    if command -v "$1" >/dev/null; then
        return 0
    fi
    if [ "${COMPAT_SKIP_MISSING:-}" = 1 ]; then
        echo "SKIP: $1 is not installed"
        return 1
    fi
    echo "$1 is not installed (set COMPAT_SKIP_MISSING=1 to skip)" >&2
    exit 2
}

run_scenario() {
    local scenario="$1"
    case "$scenario" in
        java-*)
            local version="${scenario#java-}" jar classes="$work/java-$scenario"
            jar="$(hazelcast_jar "$version")"
            javac -d "$classes" -cp "$jar" "$tests/java/src/main/java/compat/JavaClientCompat.java"
            java -cp "$classes:$jar" compat.JavaClientCompat "$scenario"
            ;;
        python)
            has python3 || return 0
            python3 -m venv "$work/venv"
            "$work/venv/bin/pip" install -q -r "$tests/python/requirements.txt"
            "$work/venv/bin/python" "$tests/python/compat.py" python
            ;;
        nodejs)
            has node || return 0
            cp -r "$tests/nodejs" "$work/nodejs"
            (cd "$work/nodejs" && npm install --no-audit --no-fund --silent)
            node "$work/nodejs/compat.js" nodejs
            ;;
        go)
            has go || return 0
            (cd "$tests/go" && go run . go)
            ;;
        dotnet)
            has dotnet || return 0
            cp -r "$tests/dotnet" "$work/dotnet"
            dotnet run --project "$work/dotnet" -- dotnet
            ;;
        hazelcast-member)
            local jar hz_pid
            jar="$(hazelcast_jar 5.7.0)"
            java -Dhazelcast.config="$tests/member/hazelcast-member.yaml" -cp "$jar" \
                com.hazelcast.core.server.HazelcastMemberStarter > "$work/hazelcast-member.log" 2>&1 &
            hz_pid=$!
            sleep 20
            kill "$hz_pid" && wait "$hz_pid" 2>/dev/null || true
            if grep -q "Members {size:2" "$work/member.log"; then
                echo "the Hazelcast member joined the Hecaton cluster"
                return 1
            fi
            grep -q "A Hazelcast member tried to connect" "$work/member.log" || {
                echo "the Hecaton member did not report the refused Hazelcast member"
                return 1
            }
            echo "OK hazelcast-member"
            ;;
        *)
            echo "unknown scenario: $scenario" >&2
            return 2
            ;;
    esac
}

echo "=== starting Hecaton member ($(basename "$hecaton_jar"))"
java -Dhecaton.config="$tests/member/hecaton.yaml" -cp "$hecaton_jar:$sql_jar" \
    uk.org.hecaton.core.server.HecatonMemberStarter > "$work/member.log" 2>&1 &
member_pid=$!
for _ in $(seq 1 60); do
    grep -q "is STARTED" "$work/member.log" && break
    sleep 1
done
grep -q "is STARTED" "$work/member.log" || { cat "$work/member.log"; echo "member did not start"; exit 1; }

failed=()
for scenario in "${scenarios[@]}"; do
    echo "=== $scenario"
    if ! output="$(run_scenario "$scenario" 2>&1)"; then
        echo "$output" | tail -n 40
        echo "FAIL $scenario"
        failed+=("$scenario")
    elif grep -q "^SKIP" <<<"$output"; then
        grep "^SKIP" <<<"$output"
    elif grep -qx "OK $scenario" <<<"$output"; then
        echo "OK $scenario"
    else
        echo "$output" | tail -n 40
        echo "FAIL $scenario: no OK line"
        failed+=("$scenario")
    fi
done

if [ ${#failed[@]} -gt 0 ]; then
    echo "--- member log (last 60 lines)"
    tail -n 60 "$work/member.log"
    echo "Failed: ${failed[*]}"
    exit 1
fi
echo "All compatibility scenarios passed: ${scenarios[*]}"
