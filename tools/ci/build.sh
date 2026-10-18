#!/usr/bin/env bash
#
# Main CI build on JDK 25.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
java -version

if [ ! -f pom.xml ]; then
    echo "No Maven project yet (the code base is imported in milestone M1). Skipping build."
    exit 0
fi

./mvnw -B -ntp verify
