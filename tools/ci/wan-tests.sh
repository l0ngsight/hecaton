#!/usr/bin/env bash
#
# WAN replication between clusters of the Hecaton container image (wan-tests/, spec §10).
#
# Usage: tools/ci/wan-tests.sh   (after the distribution and core were built and installed,
#                                 e.g. ./mvnw install -DskipTests -pl distribution -am)
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
slim="$(ls distribution/target/hecaton-*-slim.tar.gz | head -n 1)"
version="$(basename "$slim" -slim.tar.gz)"
version="${version#hecaton-}"
image="hecaton/hecaton:$version"

docker build --build-arg HECATON_VERSION="$version" -t "$image" -f distribution/src/docker/Dockerfile distribution/target
./mvnw -B -ntp -f wan-tests/pom.xml verify -Dhecaton.version="$version" -Dhecaton.image="$image"
