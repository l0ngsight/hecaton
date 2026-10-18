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
| `upstream/main` | Unmodified snapshots of Hazelcast CE releases, one commit per release tag. Never edited by hand. |
| `rebrand/vX.Y.Z` | Output of `tools/rebrand` for an upstream release. Generated, never edited by hand. |
| `sync/vX.Y.Z` | Merging a new upstream release into `main` |

## 5. Changing upstream (Hazelcast-derived) code

Keep changes to Hazelcast-derived files to a minimum. Put new features in Hecaton-owned
modules that plug into the existing SPI (see [spec §7](docs/SPEC.md#7-extension-design)).
If you must change an upstream file, add an entry to `docs/upstream-patches.md` in the same
PR explaining why. CI will enforce this once the code is imported.

## 6. Build requirements

- JDK 25 (Temurin recommended) for building.
- Modules derived from Hazelcast and all client-side code compile with `--release 17`.
  Member-only `hecaton-ext-*` modules compile with `--release 25`.
- Hecaton-owned code must not use `sun.misc.Unsafe`. Use `VarHandle` or the FFM API.

## 7. Pull requests

- Keep PRs focused: one task from the implementation plan per PR.
- Include tests. Features need multi-member and failure tests.
- CI must be green before review.
- Write commit messages in the imperative mood ("Add TLS handshake timeout").
