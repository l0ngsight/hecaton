#!/usr/bin/env bash
#
# Builds the Hecaton container image from the slim distribution (plan §1.2), starts it and
# connects a client to it.
#
# Usage: tools/ci/image-test.sh   (after the distribution was built, e.g. by tools/ci/build.sh)
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
slim="$(ls distribution/target/hecaton-*-slim.tar.gz | head -n 1)"
version="$(basename "$slim" -slim.tar.gz)"
version="${version#hecaton-}"
image="hecaton/hecaton:$version"

docker build --build-arg HECATON_VERSION="$version" -t "$image" -f distribution/src/docker/Dockerfile distribution/target
container="$(docker run -d -p 5701:5701 "$image")"
trap 'docker logs "$container" | tail -n 60; docker rm -f "$container" >/dev/null' EXIT

for _ in $(seq 1 60); do
    docker logs "$container" 2>&1 | grep -q "is STARTED" && break
    sleep 1
done
docker logs "$container" 2>&1 | grep -q "is STARTED" || { echo "member did not start"; exit 1; }

jar="$(ls hecaton/target/hecaton-*.jar | grep -vE -- '-(sources|tests|javadoc)\.jar$' | head -n 1)"
java -cp "$jar" tools/ci/ImageSmoke.java 127.0.0.1:5701
