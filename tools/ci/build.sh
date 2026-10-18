#!/usr/bin/env bash
#
# Main CI build on JDK 25: compiles every module, including tests, and packages the jars.
# Test execution is added once the baseline comparison against upstream is in place
# (implementation plan §1.2, step 4).
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
java -version

./mvnw -B -ntp -T 1C -DskipTests install
