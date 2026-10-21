#!/usr/bin/env bash
#
# Builds the member image of a release from the slim distribution and pushes it as
# <repository>:<version> and, for a final (non pre-release) version, <repository>:latest.
#
# Usage: tools/release/publish-image.sh <version> <repository>   (after tools/release/release.sh)
#   e.g. tools/release/publish-image.sh 1.0.0 ghcr.io/l0ngsight/hecaton
set -euo pipefail

version="${1:?usage: tools/release/publish-image.sh <version> <repository>}"
repository="${2:?usage: tools/release/publish-image.sh <version> <repository>}"

cd "$(git rev-parse --show-toplevel)"
tags=(-t "$repository:$version")
[[ "$version" == *-* ]] || tags+=(-t "$repository:latest")

docker buildx build --push --platform linux/amd64,linux/arm64 \
    --build-arg HECATON_VERSION="$version" "${tags[@]}" \
    --label org.opencontainers.image.revision="$(git rev-parse HEAD)" \
    -f distribution/src/docker/Dockerfile distribution/target
