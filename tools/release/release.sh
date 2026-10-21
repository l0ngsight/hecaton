#!/usr/bin/env bash
#
# Builds a Hecaton release from the current checkout: sets the version, builds and signs every
# module (profile `release`), writes the SBOM, and collects what the GitHub release carries in
# target/release: the distributions, the migration CLI, the SBOM, the release notes and checksums.
#
# Usage: tools/release/release.sh <version> [--deploy] [--no-sign]
#
#   --deploy   also upload the artifacts to Maven Central (profile `enable-maven-central`; needs
#              the `release-repository` server credentials in settings.xml)
#   --no-sign  skip GPG signing, for a dry run without the release key
#
# See docs/releasing.md.
set -euo pipefail

version="${1:?usage: tools/release/release.sh <version> [--deploy] [--no-sign]}"
shift
deploy=false
sign=true
for arg in "$@"; do
    case "$arg" in
        --deploy) deploy=true ;;
        --no-sign) sign=false ;;
        *) echo "unknown option $arg" >&2; exit 2 ;;
    esac
done
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.]+)?$ ]] || { echo "not a version: $version" >&2; exit 2; }

cd "$(git rev-parse --show-toplevel)"
java -version

./mvnw -B -ntp versions:set -DnewVersion="$version" -DgenerateBackupPoms=false -DprocessAllModules=true

profiles=release
goal=install
if $deploy; then
    profiles=release,enable-maven-central
    goal=deploy
fi
options=()
$sign || options+=(-Dgpg.skip=true)
./mvnw -B -ntp -P"$profiles" -DskipTests "${options[@]}" "$goal"

out=target/release
rm -rf "$out"
mkdir -p "$out"
cp distribution/target/hecaton-"$version"*.zip distribution/target/hecaton-"$version"*.tar.gz "$out"/
cp hecaton-migrate/target/hecaton-migrate-"$version"-cli.jar "$out"/
cp target/hecaton-"$version"-sbom.json target/hecaton-"$version"-sbom.xml "$out"/

previous="$(git describe --tags --abbrev=0 --match 'v*' HEAD^ 2>/dev/null || true)"
python3 tools/release/release-notes.py "$version" "$previous" HEAD > "$out"/RELEASE_NOTES.md
(cd "$out" && sha256sum -- *.zip *.tar.gz *.jar *.json *.xml > SHA256SUMS)
ls -l "$out"
