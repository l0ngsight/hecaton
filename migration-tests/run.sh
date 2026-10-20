#!/usr/bin/env bash
#
# Migration acceptance test (spec §5.5, plan §1.5).
#
# For each sample: run the Hazelcast application in before/ against Hazelcast 5.7, migrate a
# copy with the OpenRewrite recipe and hecaton-migrate, require the result to equal expected/
# (so nothing beyond the tool output may change), then build and run the migrated application
# against this build of Hecaton.
#
# Needs the Hecaton artifacts in the local Maven repository:
#   ./mvnw -B -ntp install -DskipTests -pl hecaton-migrate-rewrite,hecaton-sql,hecaton-spring-boot-autoconfiguration/hecaton-spring-boot3 -am
#
# Usage: migration-tests/run.sh [--update] [SAMPLE...]
#   --update  replace expected/ with the tool output (review the diff before committing)
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
tests="$root/migration-tests"
mvn=("$root/mvnw" -B -ntp -q)
rewrite_plugin="org.openrewrite.maven:rewrite-maven-plugin:6.46.1"

update=false
samples=()
for arg in "$@"; do
    case "$arg" in
        --update) update=true ;;
        *) samples+=("$arg") ;;
    esac
done
if [ ${#samples[@]} -eq 0 ]; then
    for dir in "$tests"/*/before; do
        samples+=("$(basename "$(dirname "$dir")")")
    done
fi

migrate_jar="$(ls "$root"/hecaton-migrate/target/hecaton-migrate-*.jar | grep -vE -- '-(sources|tests|javadoc)\.jar$' | head -n 1)"
version="$(basename "$migrate_jar" .jar)"
version="${version#hecaton-migrate-}"

# Builds the Maven project in $1 and runs org.example.migration.App; it must print "OK <sample>".
run_app() {
    local dir="$1" sample="$2"
    "${mvn[@]}" -f "$dir/pom.xml" package dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
    local output jvm_args=()
    if [ -f "$dir/target/classes/logging.properties" ]; then
        jvm_args+=("-Djava.util.logging.config.file=$dir/target/classes/logging.properties")
    fi
    output="$(java "${jvm_args[@]}" -cp "$dir/target/classes:$(cat "$dir/target/classpath.txt")" \
        org.example.migration.App)"
    echo "$output"
    grep -qx "OK $sample" <<<"$output"
}

# Outside the repository: the rewrite plugin skips files that the repository's .gitignore
# matches (such as anything under target/).
work_root="$(mktemp -d)"
trap 'rm -rf "$work_root"' EXIT

failed=()
for sample in "${samples[@]}"; do
    echo "=== $sample"
    work="$work_root/$sample"
    mkdir -p "$work"

    echo "--- before: Hazelcast 5.7"
    cp -r "$tests/$sample/before" "$work/before"
    run_app "$work/before" "$sample"

    echo "--- migrate to Hecaton $version"
    cp -r "$tests/$sample/before" "$work/migrated"
    "${mvn[@]}" -f "$work/migrated/pom.xml" "$rewrite_plugin:run" \
        -Drewrite.recipeArtifactCoordinates="uk.org.hecaton:hecaton-migrate-rewrite:$version" \
        -Drewrite.activeRecipes=uk.org.hecaton.migrate.FromHazelcast
    rm -rf "$work/migrated/target"
    java -jar "$migrate_jar" --apply "$work/migrated"

    echo "--- compare with expected/"
    # expected/ holds @HECATON_VERSION@ where the tools write the Hecaton version
    if $update; then
        rm -rf "$tests/$sample/expected"
        cp -r "$work/migrated" "$tests/$sample/expected"
        grep -rlF -- "$version" "$tests/$sample/expected" | xargs -r sed -i "s/$(sed 's/[.]/\\./g' <<<"$version")/@HECATON_VERSION@/g"
    fi
    cp -r "$tests/$sample/expected" "$work/expected"
    grep -rlF "@HECATON_VERSION@" "$work/expected" | xargs -r sed -i "s/@HECATON_VERSION@/$version/g"
    if ! diff -ru "$work/expected" "$work/migrated"; then
        echo "FAIL $sample: the migrated sources differ from expected/"
        failed+=("$sample")
        continue
    fi

    echo "--- after: Hecaton $version"
    if ! run_app "$work/migrated" "$sample"; then
        echo "FAIL $sample: the migrated application did not run"
        failed+=("$sample")
    fi
done

if [ ${#failed[@]} -gt 0 ]; then
    echo "Failed: ${failed[*]}"
    exit 1
fi
echo "All migration samples passed: ${samples[*]}"
