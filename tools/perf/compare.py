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
"""Compares Hecaton with Hazelcast CE in a JMH result file of benchmarks/ (docs/performance.md).

    tools/perf/compare.py results.json [--max-regression 0.05]

Prints a Markdown table of the throughput of both products for every benchmark and parameter
combination, and exits with status 1 if Hecaton is slower than Hazelcast by more than the
allowed share in any of them.
"""

import argparse
import json
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("results")
    parser.add_argument("--max-regression", type=float, default=0.05)
    args = parser.parse_args()

    scores = {}
    for result in json.load(open(args.results)):
        params = dict(result.get("params", {}))
        product = params.pop("product")
        key = (result["benchmark"].rsplit(".", 1)[-1], tuple(sorted(params.items())))
        metric = result["primaryMetric"]
        # the error is "NaN" (a string) when there were too few iterations to compute it
        scores.setdefault(key, {})[product] = (float(metric["score"]), float(metric["scoreError"]),
                                               metric["scoreUnit"])

    print("| Benchmark | Parameters | Hazelcast CE | Hecaton | Hecaton / Hazelcast |")
    print("|---|---|---|---|---|")
    failed = []
    for (benchmark, params), products in sorted(scores.items()):
        if "hecaton" not in products or "hazelcast" not in products:
            continue
        hz, hz_error, unit = products["hazelcast"]
        hc, hc_error, _ = products["hecaton"]
        ratio = hc / hz
        parameters = ", ".join(f"{name}={value}" for name, value in params)
        print(f"| {benchmark} | {parameters} | {hz:,.0f} ± {hz_error:,.0f} {unit} "
              f"| {hc:,.0f} ± {hc_error:,.0f} {unit} | {ratio:.3f} |")
        if ratio < 1 - args.max_regression:
            failed.append(f"{benchmark} ({parameters})")
    if failed:
        print(f"\nSlower than Hazelcast by more than {args.max_regression:.0%}: " + "; ".join(failed))
        sys.exit(1)


if __name__ == "__main__":
    main()
