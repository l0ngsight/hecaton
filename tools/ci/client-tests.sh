#!/usr/bin/env bash
#
# Client test suite, run on JDK 17, 21 and 25.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
java -version

if [ ! -f pom.xml ]; then
    echo "No Maven project yet (the client library arrives in milestone M1). Skipping client tests."
    exit 0
fi

# The build itself needs JDK 25 (toolchain); tests run on the JDK selected by the matrix.
./mvnw -B -ntp -Pclient-tests verify
