#!/usr/bin/env python3
#
# Copyright (c) 2026, The Hecaton Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Writes the release notes of a Hecaton release, in Markdown, from the git history.

    tools/release/release-notes.py <version> [<previous tag>] [<ref>]

The notes list the pull requests merged into main since the previous tag (their titles come
from the merge commits), then the changes committed directly to main. Commit subjects in the
conventional form (`feat: ...`, `fix(wan): ...`) are grouped by type; the others, written as
`Area: summary`, by area. Without a previous tag the whole history is used.
"""

import re
import subprocess
import sys
from collections import OrderedDict

CONVENTIONAL = re.compile(r"^(?P<type>[a-z]+)(\((?P<scope>[^)]+)\))?(?P<breaking>!)?: (?P<subject>.+)$")
AREA = re.compile(r"^(?P<area>[A-Z][\w ./-]{0,40}): (?P<subject>.+)$")
PR_MERGE = re.compile(r"^Merge pull request #(?P<number>\d+) from \S+$")
SQUASH_PR = re.compile(r"^(?P<subject>.+) \(#(?P<number>\d+)\)$")
TYPE_TITLES = OrderedDict([
    ("feat", "Features"),
    ("fix", "Fixes"),
    ("perf", "Performance"),
    ("docs", "Documentation"),
    ("build", "Build"),
    ("ci", "CI"),
    ("refactor", "Refactoring"),
    ("test", "Tests"),
    ("chore", "Other changes"),
])


def git(*args):
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def first_parent_commits(revision_range):
    """(sha, subject, body) of the commits on the first-parent line of the range, oldest first."""
    out = git("log", "--first-parent", "--reverse", "--format=%H%x1f%s%x1f%b%x1e", revision_range)
    commits = []
    for record in out.split("\x1e"):
        record = record.strip("\n")
        if record:
            sha, subject, body = record.split("\x1f")
            commits.append((sha, subject, body.strip()))
    return commits


def classify(subject):
    """(section, text) for a commit subject."""
    match = CONVENTIONAL.match(subject)
    if match:
        section = TYPE_TITLES.get(match["type"], "Other changes")
        scope = f"**{match['scope']}**: " if match["scope"] else ""
        breaking = " (breaking)" if match["breaking"] else ""
        return section, f"{scope}{match['subject']}{breaking}"
    match = AREA.match(subject)
    if match:
        return match["area"], match["subject"]
    return "Other changes", subject


def notes(version, previous, ref):
    revision_range = f"{previous}..{ref}" if previous else ref
    pull_requests = []
    sections = OrderedDict()
    for sha, subject, body in first_parent_commits(revision_range):
        merge = PR_MERGE.match(subject)
        squash = SQUASH_PR.match(subject)
        if merge:
            title = body.splitlines()[0] if body else subject
            pull_requests.append((merge["number"], title))
        elif squash:
            pull_requests.append((squash["number"], squash["subject"]))
        elif not subject.startswith("Merge "):
            section, text = classify(subject)
            sections.setdefault(section, []).append(f"{text} ({sha[:7]})")

    lines = [f"# Hecaton {version}", ""]
    if previous:
        lines += [f"Changes since {previous}.", ""]
    if pull_requests:
        lines += ["## Pull requests", ""]
        lines += [f"- {title} (#{number})" for number, title in pull_requests]
        lines.append("")
    for section, items in sections.items():
        lines += [f"## {section}", ""]
        lines += [f"- {item}" for item in items]
        lines.append("")
    lines += [
        "Hecaton members need Java 25; the client library runs on Java 17 and later. See",
        "[changes from Hazelcast](https://github.com/l0ngsight/hecaton/blob/main/docs/changes-from-hazelcast.md)",
        "for what differs from Hazelcast 5.7.",
        "",
    ]
    return "\n".join(lines)


def main(argv):
    if not 2 <= len(argv) <= 4:
        sys.exit(__doc__)
    version = argv[1]
    previous = argv[2] if len(argv) > 2 and argv[2] else None
    ref = argv[3] if len(argv) > 3 else "HEAD"
    sys.stdout.write(notes(version, previous, ref))


if __name__ == "__main__":
    main(sys.argv)
