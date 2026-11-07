#!/usr/bin/env bash
#
# The Java oracle of hecaton-rust (its plan §9.1). Two modes:
#
#   rust-oracle.sh protocol SAMPLES_DIR OUT_DIR
#     Checks the Rust client-protocol codecs against the Java codecs (RustProtocolOracle.java).
#     SAMPLES_DIR is written by
#       cargo run -p hecaton-protocol --features samples --example write-samples -- DIR
#     OUT_DIR receives the Java-encoded fixtures and the manifest (hecaton-rust: fixtures/protocol).
#
#   rust-oracle.sh serialization OUT_DIR
#     Writes the serialization fixtures (RustSerializationOracle.java; hecaton-rust:
#     fixtures/serialization).
#
# Uses hecaton/target/hecaton-*.jar when it exists (`./mvnw -pl hecaton-tpc-engine,hecaton install
# -DskipTests`), otherwise the released jar from Maven Central given by HECATON_VERSION.
set -euo pipefail

# Paths are relative to the caller's directory; the script finds the Java repository from its
# own location, so it can run from a hecaton-rust checkout.
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
cache="${XDG_CACHE_HOME:-$HOME/.cache}/hecaton/rust-oracle"
mkdir -p "$cache"

central=https://repo.maven.apache.org/maven2
# Downloads a jar from Maven Central into the cache, checks its SHA-1 and prints its path.
fetch() { # group artifact version
    local path="${1//.//}/$2/$3/$2-$3.jar" jar="$cache/$2-$3.jar" sum attempt
    if [ ! -f "$jar" ]; then
        for attempt in 1 2 3 4; do
            if curl -fsS -o "$jar.part" "$central/$path" && sum="$(curl -fsS "$central/$path.sha1")"; then
                break
            fi
            rm -f "$jar.part"
            [ "$attempt" = 4 ] && { echo "cannot download $path" >&2; return 1; }
            sleep $((attempt * 2))
        done
        echo "${sum%% *}  $jar.part" | sha1sum -c --quiet >&2 || { rm -f "$jar.part"; return 1; }
        mv "$jar.part" "$jar"
    fi
    echo "$jar"
}

hecaton_jar="$(ls "$root"/hecaton/target/hecaton-*.jar 2>/dev/null | grep -vE -- '-(sources|tests|javadoc)\.jar$' | head -n 1 || true)"
if [ -z "$hecaton_jar" ]; then
    hecaton_jar="$(fetch uk.org.hecaton hecaton "${HECATON_VERSION:-1.0.0}")" || exit 1
fi
byte_buddy_jar="$(fetch net.bytebuddy byte-buddy 1.17.8)" || exit 1
echo "using $hecaton_jar" >&2
mode="${1:?usage: rust-oracle.sh protocol SAMPLES_DIR OUT_DIR | serialization OUT_DIR}"
shift
case "$mode" in
    protocol) java -cp "$hecaton_jar:$byte_buddy_jar" "$here/RustProtocolOracle.java" "$@" ;;
    serialization) java -cp "$hecaton_jar" "$here/RustSerializationOracle.java" "$@" ;;
    *) echo "unknown mode $mode" >&2; exit 2 ;;
esac
