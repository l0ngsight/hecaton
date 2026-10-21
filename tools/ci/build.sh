#!/usr/bin/env bash
#
# Main CI build on JDK 25: compiles every module, including tests, and packages the jars.
# The test suites run in the nightly workflow; only hecaton-hd's and hecaton-persistence's tests run here.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
java -version

./mvnw -B -ntp -T 1C -DskipTests install

# hecaton-hd's own tests are quick, and native memory bugs can crash a JVM: run them on every build;
# so are hecaton-persistence's
./mvnw -B -ntp -pl hecaton-hd,hecaton-persistence surefire:test
