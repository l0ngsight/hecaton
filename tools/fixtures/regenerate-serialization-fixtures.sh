#!/usr/bin/env bash
#
# Regenerates the serialization fixtures that embed class names (see
# RegenerateSerializationFixtures.java). Needs JDK 25 and a prior
# `./mvnw -pl hecaton-tpc-engine,hecaton install -DskipTests`.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
java_bin="${JAVA_HOME:+$JAVA_HOME/bin/}java"
cp_file="$(mktemp)"
trap 'rm -f "$cp_file"' EXIT
./mvnw -B -q -ntp -pl hecaton dependency:build-classpath -Dmdep.includeScope=test -Dmdep.outputFile="$cp_file"
classpath="$PWD/hecaton/target/test-classes:$PWD/hecaton/target/classes:$(cat "$cp_file")"
script="$PWD/tools/fixtures/RegenerateSerializationFixtures.java"
(cd hecaton/src/test/resources && "$java_bin" -cp "$classpath" "$script")
