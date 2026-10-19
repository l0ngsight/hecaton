#!/usr/bin/env bash
#
# Rebuilds the JNI helper libraries shipped in hecaton-tpc-engine from their C sources
# (hecaton/src/main/resources/*.c), using the flags documented next to the sources
# (affinity_helper.md, icmp_helper.md). Upstream ships prebuilt binaries whose JNI
# symbols carry the old package name, so Hecaton rebuilds them (docs/changes-from-hazelcast.md).
#
# Requires gcc with 32-bit support (Debian/Ubuntu: gcc-multilib) and a JDK (JAVA_HOME).
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
java_home="${JAVA_HOME:?set JAVA_HOME to a JDK}"
includes=(-I "$java_home/include" -I "$java_home/include/linux")
src=hecaton/src/main/resources
out=hecaton-tpc-engine/src/main/resources/lib
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# build <arch-dir> <-m64|-m32> <name> [extra cflags...]
build() {
    local arch="$1" bits="$2" name="$3"
    shift 3
    gcc "$bits" -c "${includes[@]}" -fPIC "$@" -o "$work/$name-$arch.o" "$src/$name.c"
    gcc "$bits" -shared -fPIC -Wl,-soname,"lib$name.so" -o "$out/$arch/lib$name.so" "$work/$name-$arch.o" -lc
    strip --strip-unneeded "$out/$arch/lib$name.so"
    echo "built $out/$arch/lib$name.so"
}

mkdir -p "$out/linux-x86_64" "$out/linux-x86"
build linux-x86_64 -m64 affinity_helper -Os
build linux-x86_64 -m64 icmp_helper
build linux-x86 -m32 icmp_helper

tools/ci/check-native.sh
