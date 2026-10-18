#!/usr/bin/env bash
#
# Imports a snapshot of a Hazelcast CE release tag onto the `upstream/main`
# branch, one commit per tag, without upstream history. Files are unmodified;
# paths listed in tools/upstream/excluded-paths.txt are left out.
#
# Usage: tools/upstream/import-snapshot.sh <tag>      e.g. v5.7.0
#
# The script only creates a local commit. Review it, then push with:
#   git push origin upstream/main
set -euo pipefail

UPSTREAM_URL="${UPSTREAM_URL:-https://github.com/hazelcast/hazelcast.git}"
BRANCH="upstream/main"

tag="${1:?usage: $0 <tag>}"
repo_root="$(git rev-parse --show-toplevel)"
excludes_file="$repo_root/tools/upstream/excluded-paths.txt"

pathspec=(.)
while IFS= read -r line; do
    line="${line%%#*}"
    line="${line//[[:space:]]/}"
    [ -n "$line" ] && pathspec+=(":(glob,exclude)$line")
done < "$excludes_file"
excludes_sha="$(sha256sum "$excludes_file" | cut -d' ' -f1)"
work="$(mktemp -d)"
trap 'git -C "$repo_root" worktree remove --force "$work/tree" >/dev/null 2>&1 || true; rm -rf "$work"' EXIT

echo "Fetching $UPSTREAM_URL @ $tag"
git clone --quiet --depth 1 --branch "$tag" "$UPSTREAM_URL" "$work/src" 2>/dev/null
commit="$(git -C "$work/src" rev-parse HEAD)"

if git -C "$repo_root" rev-parse --verify --quiet "refs/heads/$BRANCH" >/dev/null; then
    git -C "$repo_root" worktree add --quiet "$work/tree" "$BRANCH"
elif git -C "$repo_root" rev-parse --verify --quiet "refs/remotes/origin/$BRANCH" >/dev/null; then
    git -C "$repo_root" worktree add --quiet -b "$BRANCH" "$work/tree" "origin/$BRANCH"
else
    git -C "$repo_root" worktree add --quiet --detach "$work/tree"
    git -C "$work/tree" switch --quiet --orphan "$BRANCH"
fi

last_msg="$(git -C "$work/tree" log -1 --format=%B 2>/dev/null || true)"
if grep -q "^Upstream-Commit: $commit$" <<<"$last_msg" \
        && grep -q "^Upstream-Excludes-SHA256: $excludes_sha$" <<<"$last_msg"; then
    echo "$BRANCH already contains $tag ($commit) with the current exclusions; nothing to do."
    exit 0
fi

# Replace the tree with the files tracked upstream at the tag, minus exclusions.
git -C "$work/tree" rm -r --quiet --ignore-unmatch -- . >/dev/null
excluded_count=$(( $(git -C "$work/src" ls-files | wc -l) - $(git -C "$work/src" ls-files -- "${pathspec[@]}" | wc -l) ))
git -C "$work/src" ls-files -z -- "${pathspec[@]}" | (cd "$work/src" && xargs -0 cp --parents -a -t "$work/tree")
git -C "$work/tree" add --all --force

git -C "$work/tree" commit --quiet -F - <<MSG
Import Hazelcast CE $tag snapshot

Tree of $UPSTREAM_URL at $tag, imported without upstream history.
Files are unmodified. $excluded_count upstream files are left out as listed in
tools/upstream/excluded-paths.txt.

Upstream-Tag: $tag
Upstream-Commit: $commit
Upstream-Excludes-SHA256: $excludes_sha
MSG

echo "Committed $tag ($commit) to $BRANCH: $(git -C "$work/tree" rev-parse --short HEAD)"
