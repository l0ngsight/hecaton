#!/usr/bin/env bash
#
# Client checks on the JDK selected by the CI matrix (17, 21, 25). Uses the jar built on
# JDK 25 by the build job (bytecode level 17).
#
# Usage: tools/ci/client-tests.sh [path/to/hecaton-VERSION.jar]
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
java -version

jar="${1:-$(ls hecaton/target/hecaton-*.jar | grep -vE -- '-(sources|tests|javadoc)\.jar$' | head -n 1)}"
java -cp "$jar" tools/ci/ClientSmoke.java
