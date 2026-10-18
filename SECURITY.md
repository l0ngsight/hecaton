# Security Policy

## Reporting a vulnerability

Please **do not** open a public issue for security problems.

Report vulnerabilities privately through GitHub's
[private vulnerability reporting](https://github.com/l0ngsight/hecaton/security/advisories/new).
Include affected versions, a description, and reproduction steps if you have them.

We aim to acknowledge reports within 3 business days and to agree on a disclosure timeline
with you.

## Supported versions

Hecaton has not released a version yet. Once it does, this section will list the versions
that receive security fixes.

## Upstream fixes

Hecaton is based on Hazelcast Community Edition. Hazelcast no longer publishes Community
Edition patch releases, so Hecaton backports relevant upstream security fixes itself
(see the [implementation plan](docs/IMPLEMENTATION_PLAN.md#continuous-workstream--upstream-sync)).
If you know of an upstream fix that Hecaton has not picked up, please report it the same way.
