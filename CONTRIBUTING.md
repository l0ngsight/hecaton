# Contributing to Hecaton

Thanks for helping. Please read this page before you open a pull request. The clean-room
rules below are mandatory.

## 1. Clean-room policy (mandatory)

Hecaton re-implements features that Hazelcast, Inc. sells only in Hazelcast Enterprise
Edition. For this to be legally safe, every contribution must be written independently.

**You must not**, for any work on Hecaton:

- read, copy, or paraphrase Hazelcast Enterprise source code;
- decompile, disassemble, or inspect Hazelcast Enterprise jars, class files, Docker images,
  or native libraries;
- run Hazelcast Enterprise to observe internal behaviour (wire traffic, on-disk formats,
  logs) for the purpose of reproducing it;
- contribute if you have had access to Hazelcast Enterprise source code under an
  employment, contractor, or customer agreement. Talk to the maintainers first.

**You may** use:

- the Apache-2.0 licensed Hazelcast Community Edition code, including historical tags
  (for example `v5.4.0`, the last release with the open CP Subsystem);
- Hazelcast's **public** documentation, reference manuals, public XSD/YAML schemas,
  blog posts and conference talks;
- academic papers and textbooks (Raft, log-structured storage, Merkle trees, HNSW, ...);
- third-party libraries under Apache 2.0, MIT, BSD or other compatible licenses.

Every pull request template has a provenance checkbox. Ticking it is a statement that the
contribution follows this policy.

## 2. Licensing of contributions

- Hecaton is licensed under the [Apache License 2.0](LICENSE). By contributing, you agree
  that your contribution is licensed under the same terms.
- **New files** start with the header in
  [`tools/license/hecaton-header.txt`](tools/license/hecaton-header.txt).
- **Files that come from Hazelcast** keep their original copyright header. Never remove or
  change it.
- Files under the Hazelcast Community License (the SQL module and some connectors) stay
  under that license. Hecaton core and the `hecaton-ext-*` modules must not depend on them.

## 3. Developer Certificate of Origin

Every commit must be signed off (`git commit -s`), certifying the
[Developer Certificate of Origin 1.1](https://developercertificate.org/):

```
Signed-off-by: Your Name <you@example.com>
```

## 4. Branching

| Branch | Purpose |
|---|---|
| `main` | Integrated, releasable state |
| `feature/mN-*` | One long-lived branch per milestone (see [implementation plan](docs/IMPLEMENTATION_PLAN.md#branching-model)). Task PRs target the milestone branch. |
| `upstream/main`, `rebrand/v5.7.0` | Read-only record of how the fork was made: the Hazelcast CE 5.7.0 snapshot and its renamed form. Hecaton does not merge later upstream releases ([spec §9](docs/SPEC.md#9-relationship-with-upstream-d10)). |

## 5. Changing Hazelcast-derived code

The whole code base is Hecaton's: change, refactor or delete inherited code when that makes
Hecaton better (see [spec §4 and §7](docs/SPEC.md#7-extension-design)). Keep the original
copyright header on files derived from Hazelcast.

If a change is visible to applications migrating from Hazelcast 5.7 (API, config, wire,
REST, metrics, file names), add it to `docs/changes-from-hazelcast.md` in the same PR.

## 6. Build requirements

- JDK 25 (Temurin recommended) for building.
- The core module `hecaton` (member and client in one jar) and the other inherited modules
  compile with `--release 17`. Member-only modules (`hecaton-hd`, `hecaton-persistence`, ...)
  compile with `--release 25`.
- New code must not use `sun.misc.Unsafe`. Use `VarHandle` or the FFM API.

## 7. Pull requests

- Keep PRs focused: one task from the implementation plan per PR.
- Include tests. Features need multi-member and failure tests.
- CI must be green before review.
- Write commit messages in the imperative mood ("Add TLS handshake timeout").
