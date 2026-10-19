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
"""Helpers for the nightly test workflow (.github/workflows/nightly.yml).

  nightly.py matrix                         JSON job matrix (stdout)
  nightly.py modules --suite S --group G    Maven -pl value for a group
  nightly.py shard --root DIR --module M --shard I --shards N
                                            comma-separated test classes for one shard
  nightly.py report --reports DIR --suite S [--baseline F] [--known F]
                    [--failures-out F] [--summary-out F]
                                            merge JUnit XML, classify failures
"""

import argparse
import glob
import json
import os
import re
import sys
import xml.etree.ElementTree as ET

# Logical test groups. "shards" > 1 splits one module's test classes across jobs;
# groups with several modules run all their tests in one job.
GROUPS = [
    {"group": "core", "shards": 20, "modules": {"hecaton": ["hecaton"], "upstream": ["hazelcast"]}},
    {"group": "sql", "shards": 6, "modules": {"hecaton": ["hecaton-sql"], "upstream": ["hazelcast-sql"]}},
    {"group": "spring", "shards": 1, "modules": {
        "hecaton": ["hecaton-spring-tests", "hecaton-spring-boot-autoconfiguration/hecaton-spring-boot3",
                    "hecaton-spring-boot-autoconfiguration/hecaton-spring-boot4"],
        "upstream": ["hazelcast-spring-tests", "hazelcast-spring-boot-autoconfiguration/hazelcast-spring-boot3",
                     "hazelcast-spring-boot-autoconfiguration/hazelcast-spring-boot4"]}},
    {"group": "ext-streaming", "shards": 1, "modules": {s: [
        "extensions/kafka", "extensions/kafka-connect", "extensions/avro", "extensions/csv",
        "extensions/protobuf", "extensions/grpc", "extensions/python"] for s in ("hecaton", "upstream")}},
    {"group": "ext-stores", "shards": 1, "modules": {s: [
        "extensions/mongodb", "extensions/elasticsearch/elasticsearch-7", "extensions/cdc-debezium",
        "extensions/cdc-mysql", "extensions/cdc-postgres", "extensions/hadoop", "extensions/mapstore"]
        for s in ("hecaton", "upstream")}},
]

TEST_ANNOTATION = re.compile(r"@(?:Test|ParameterizedTest|RepeatedTest|TestFactory|TestTemplate)\b")


def cmd_matrix(_):
    include = []
    for g in GROUPS:
        for i in range(1, g["shards"] + 1):
            name = g["group"] if g["shards"] == 1 else f"{g['group']}-{i}"
            include.append({"name": name, "group": g["group"], "shard": i, "shards": g["shards"]})
    print(json.dumps({"include": include}))


def group_by_name(name):
    for g in GROUPS:
        if g["group"] == name:
            return g
    sys.exit(f"unknown group {name}")


def cmd_modules(args):
    print(",".join(group_by_name(args.group)["modules"][args.suite]))


def cmd_shard(args):
    """Balanced split of a module's test classes by number of test methods (greedy LPT)."""
    test_root = os.path.join(args.root, args.module, "src", "test", "java")
    classes = []
    for path in sorted(glob.glob(os.path.join(test_root, "**", "*.java"), recursive=True)):
        rel = os.path.relpath(path, test_root)
        if "/jsr/" in f"/{rel}" or rel.endswith("IT.java"):
            continue  # same exclusions as the surefire configuration
        with open(path, encoding="utf-8", errors="replace") as f:
            text = f.read()
        count = len(TEST_ANNOTATION.findall(text))
        if count == 0 or re.search(r"\babstract\s+class\b", text):
            continue
        classes.append((count, rel[:-len(".java")].replace(os.sep, ".")))
    bins = [[0, []] for _ in range(args.shards)]
    for count, name in sorted(classes, key=lambda c: (-c[0], c[1])):
        target = min(range(args.shards), key=lambda b: (bins[b][0], b))
        bins[target][0] += count
        bins[target][1].append(name)
    print(",".join(sorted(bins[args.shard - 1][1])))


def normalize(test_id):
    """Maps an upstream test id to its Hecaton name (same rules as tools/rebrand)."""
    test_id = re.sub(r"com\.hazelcast", "uk.org.hecaton", test_id)
    return test_id.replace("HAZELCAST", "HECATON").replace("Hazelcast", "Hecaton").replace("hazelcast", "hecaton")


def read_list(path):
    entries = {}
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    test, _, reason = line.partition("  ")
                    entries[test.strip()] = reason.strip()
    return entries


def cmd_report(args):
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    failing = {}
    per_group = {}
    for xml in sorted(glob.glob(os.path.join(args.reports, "**", "TEST-*.xml"), recursive=True)):
        group = os.path.relpath(xml, args.reports).split(os.sep)[0]
        try:
            # Reports come from this workflow's own surefire runs, not from outside input.
            root = ET.parse(xml).getroot()
        except ET.ParseError:
            continue
        for case in root.iter("testcase"):
            test_id = f"{case.get('classname')}#{case.get('name')}"
            stats = per_group.setdefault(group, {"tests": 0, "failed": 0, "skipped": 0})
            totals["tests"] += 1
            stats["tests"] += 1
            if case.find("skipped") is not None:
                totals["skipped"] += 1
                stats["skipped"] += 1
            elif case.find("failure") is not None or case.find("error") is not None:
                kind = "failures" if case.find("failure") is not None else "errors"
                totals[kind] += 1
                stats["failed"] += 1
                failing[normalize(test_id) if args.suite == "upstream" else test_id] = group

    baseline = read_list(args.baseline)
    known = read_list(args.known)
    upstream_too = sorted(t for t in failing if t in baseline)
    triaged = sorted(t for t in failing if t not in baseline and t in known)
    new = sorted(t for t in failing if t not in baseline and t not in known)

    if args.failures_out:
        with open(args.failures_out, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(failing)) + ("\n" if failing else ""))

    lines = [f"# Nightly test results ({args.suite})", "",
             f"- tests: {totals['tests']}, failures: {totals['failures']}, errors: {totals['errors']}, "
             f"skipped: {totals['skipped']}", ""]
    lines += ["| Shard | Tests | Failed | Skipped |", "|---|---|---|---|"]
    for group, s in sorted(per_group.items()):
        lines.append(f"| {group} | {s['tests']} | {s['failed']} | {s['skipped']} |")
    if args.suite == "hecaton":
        lines += ["", f"- failing upstream too (baseline): {len(upstream_too)}",
                  f"- known and triaged (tools/ci/known-test-failures.txt): {len(triaged)}",
                  f"- **new, untriaged: {len(new)}**"]
        if new:
            lines += ["", "## New failures", ""] + [f"- `{t}` ({failing[t]})" for t in new[:500]]
            if len(new) > 500:
                lines.append(f"- ... and {len(new) - 500} more (see the failures artifact)")
    summary = "\n".join(lines) + "\n"
    print(summary)
    if args.summary_out:
        with open(args.summary_out, "w", encoding="utf-8") as f:
            f.write(summary)
    if totals["tests"] == 0:
        sys.exit("no test results found")
    sys.exit(1 if args.suite == "hecaton" and new else 0)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("matrix")
    p = sub.add_parser("modules")
    p.add_argument("--suite", choices=["hecaton", "upstream"], required=True)
    p.add_argument("--group", required=True)
    p = sub.add_parser("shard")
    p.add_argument("--root", required=True)
    p.add_argument("--module", required=True)
    p.add_argument("--shard", type=int, required=True)
    p.add_argument("--shards", type=int, required=True)
    p = sub.add_parser("report")
    p.add_argument("--reports", required=True)
    p.add_argument("--suite", choices=["hecaton", "upstream"], required=True)
    p.add_argument("--baseline")
    p.add_argument("--known")
    p.add_argument("--failures-out")
    p.add_argument("--summary-out")
    args = parser.parse_args()
    {"matrix": cmd_matrix, "modules": cmd_modules, "shard": cmd_shard, "report": cmd_report}[args.cmd](args)


if __name__ == "__main__":
    main()
