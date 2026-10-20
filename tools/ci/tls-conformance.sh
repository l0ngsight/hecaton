#!/usr/bin/env bash
#
# TLS conformance (plan M3 tests): starts a member with TLS on a key pair made with keytool and
# scans it with testssl.sh. Fails on findings of severity MEDIUM or above, except those about the
# self-signed test certificate.
#
# Usage: tools/ci/tls-conformance.sh [path/to/hecaton-VERSION.jar]
# Needs: java 25, git, jq, and what testssl.sh needs (openssl, hexdump).
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
jar="${1:-$(ls hecaton/target/hecaton-*.jar | grep -vE -- '-(sources|tests|javadoc)\.jar$' | head -n 1)}"
testssl_version="${TESTSSL_VERSION:-v3.2.1}"
port=5781

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

keytool -genkeypair -alias member -keyalg EC -groupname secp256r1 -validity 30 \
    -dname "CN=localhost" -ext "SAN=dns:localhost,ip:127.0.0.1" \
    -keystore "$work/member.p12" -storetype PKCS12 -storepass changeit -keypass changeit

cat > "$work/member.yaml" <<YAML
hecaton:
  cluster-name: tls-conformance
  network:
    port:
      port: $port
      auto-increment: false
    join:
      multicast:
        enabled: false
      auto-detection:
        enabled: false
    ssl:
      enabled: true
      properties:
        keyStore: $work/member.p12
        keyStorePassword: changeit
        keyStoreType: PKCS12
YAML

java -Dhecaton.config="$work/member.yaml" -cp "$jar" uk.org.hecaton.core.server.HecatonMemberStarter \
    > "$work/member.log" 2>&1 &
member_pid=$!
for _ in $(seq 1 60); do
    if grep -q "is STARTED" "$work/member.log"; then
        break
    fi
    sleep 1
done
grep -q "is STARTED" "$work/member.log" || { cat "$work/member.log"; echo "the member did not start"; exit 1; }

git -c advice.detachedHead=false clone -q --depth 1 --branch "$testssl_version" \
    https://github.com/testssl/testssl.sh.git "$work/testssl"
# testssl.sh exits with the number of findings, or 242 and above on a fatal error
status=0
"$work/testssl/testssl.sh" --quiet --color 0 --warnings off --nodns none --jsonfile "$work/testssl.json" \
    "127.0.0.1:$port" || status=$?
if [ "$status" -ge 242 ] || [ ! -s "$work/testssl.json" ] || [ "$(jq length "$work/testssl.json")" -lt 20 ]; then
    echo "testssl.sh failed (exit status $status)"
    exit 1
fi

# Findings about the throwaway self-signed certificate (trust chain, validity, revocation) and the
# grade it caps are expected; everything else of severity MEDIUM or above fails.
filter='.[] | select(.severity == "MEDIUM" or .severity == "HIGH" or .severity == "CRITICAL")
    | select((.id | startswith("cert_")) or .id == "overall_grade" | not)
    | "\(.severity) \(.id): \(.finding)"'
findings="$(jq -r "$filter" "$work/testssl.json")"
if [ -n "$findings" ]; then
    echo "testssl.sh findings of severity MEDIUM or above:"
    echo "$findings"
    exit 1
fi
echo "TLS conformance: no findings of severity MEDIUM or above (apart from the test certificate)"
