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
"""
How much of the core jar a client needs (spec section 8.3, phase 2): the classes reachable from the
client entry points and from the service files the client loads, found with jdeps.

    tools/client-jar/reachability.py hecaton/target/hecaton-<version>.jar [--gateways]

--gateways also lists the non-client classes referenced by client classes whose removal would
shrink the closure the most, which are where decoupling the client would start.
"""
import argparse
import collections
import re
import shutil
import subprocess
import sys
import zipfile

ENTRY_POINTS = [
    "uk.org.hecaton.client.HecatonClient",
    "uk.org.hecaton.client.config.ClientConfig",
    "uk.org.hecaton.client.config.ClientFailoverConfig",
    "uk.org.hecaton.client.config.XmlClientConfigBuilder",
    "uk.org.hecaton.client.config.YamlClientConfigBuilder",
    "uk.org.hecaton.client.config.XmlClientFailoverConfigBuilder",
    "uk.org.hecaton.client.config.YamlClientFailoverConfigBuilder",
    "uk.org.hecaton.client.cache.HecatonClientCachingProvider",
]
# service files only members load
MEMBER_SERVICES = {
    "uk.org.hecaton.client.impl.protocol.MessageTaskFactoryProvider",
    "uk.org.hecaton.spi.impl.servicemanager.ServiceDescriptorProvider",
    "uk.org.hecaton.dataconnection.DataConnectionRegistration",
}
EDGE = re.compile(r"\s+(\S+)\s+->\s+(\S+)")


def dependency_graph(jar):
    jdeps = shutil.which("jdeps")
    if jdeps is None:
        sys.exit("jdeps (JDK) is not on the PATH")
    out = subprocess.run([jdeps, "-verbose:class", "-filter:none", "--multi-release", "25",
                          "--ignore-missing-deps", jar], capture_output=True, text=True, check=True).stdout
    edges = collections.defaultdict(set)
    classes = set()
    for line in out.splitlines():
        match = EDGE.match(line)
        if match:
            source, target = match.groups()
            classes.add(source)
            if target.startswith("uk.org.hecaton"):
                edges[source].add(target)
    for name in classes:
        if "$" in name:
            edges[name].add(name.split("$")[0])
    return classes, edges


def service_roots(jar, include_services):
    roots = list(ENTRY_POINTS)
    if not include_services:
        return roots
    with zipfile.ZipFile(jar) as archive:
        for name in archive.namelist():
            service = name[len("META-INF/services/"):]
            if name.startswith("META-INF/services/") and service and service not in MEMBER_SERVICES:
                for line in archive.read(name).decode().splitlines():
                    line = line.split("#")[0].strip()
                    if line:
                        roots.append(line)
    return roots


def closure(classes, edges, roots, banned=frozenset()):
    seen = set()
    stack = [root for root in roots if root in classes]
    while stack:
        name = stack.pop()
        if name in seen or name in banned:
            continue
        seen.add(name)
        stack.extend(edges[name])
    return seen


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("jar")
    parser.add_argument("--gateways", action="store_true")
    args = parser.parse_args()
    classes, edges = dependency_graph(args.jar)
    entry_only = closure(classes, edges, service_roots(args.jar, False))
    reachable = closure(classes, edges, service_roots(args.jar, True))
    print(f"classes in the jar:                          {len(classes)}")
    print(f"reachable from the client entry points:      {len(entry_only)} ({100 * len(entry_only) // len(classes)}%)")
    print(f"... and from the client's service files:     {len(reachable)} ({100 * len(reachable) // len(classes)}%)")
    if args.gateways:
        direct = {target for name in entry_only if name.startswith("uk.org.hecaton.client")
                  for target in edges[name] if target in entry_only and not target.startswith("uk.org.hecaton.client")}
        ranked = sorted(((len(entry_only) - len(closure(classes, edges, ENTRY_POINTS, frozenset([g]))), g)
                         for g in direct), reverse=True)
        for saved, gateway in ranked[:20]:
            print(f"{saved:6d}  {gateway}")


if __name__ == "__main__":
    main()
