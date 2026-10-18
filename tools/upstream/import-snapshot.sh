#!/usr/bin/env bash
#
# Imports an unmodified snapshot of a Hazelcast CE release tag onto the
# `upstream/main` branch, one commit per tag, without upstream history.
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

if git -C "$work/tree" log -1 --format=%B 2>/dev/null | grep -q "^Upstream-Commit: $commit$"; then
    echo "$BRANCH already contains $tag ($commit); nothing to do."
    exit 0
fi

# Replace the tree with exactly the files tracked upstream at the tag.
git -C "$work/tree" rm -r --quiet --ignore-unmatch -- . >/dev/null
git -C "$work/src" ls-files -z | (cd "$work/src" && xargs -0 cp --parents -a -t "$work/tree")
git -C "$work/tree" add --all --force

git -C "$work/tree" commit --quiet -F - <<MSG
Import Hazelcast CE $tag snapshot

Unmodified tree of $UPSTREAM_URL at $tag, imported without upstream history.

Upstream-Tag: $tag
Upstream-Commit: $commit
MSG

echo "Committed $tag ($commit) to $BRANCH: $(git -C "$work/tree" rev-parse --short HEAD)"
