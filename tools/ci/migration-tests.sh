#!/usr/bin/env bash
#
# Migration tooling checks (plan §1.5): unit tests of hecaton-migrate and
# hecaton-migrate-rewrite, then the migration acceptance test (spec §5.5), which needs the
# Hecaton artifacts in the local Maven repository.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
java -version

./mvnw -B -ntp -T 1C install -DskipTests -pl hecaton-migrate-rewrite,hecaton-sql,hecaton-spring-boot-autoconfiguration/hecaton-spring-boot3 -am
./mvnw -B -ntp test -pl hecaton-migrate,hecaton-migrate-rewrite
migration-tests/run.sh
