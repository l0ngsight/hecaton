#!/usr/bin/env bash
# Builds the hecaton.org.uk website into site/dist: the static pages plus the XML and JSON
# configuration schemas, copied from the source tree so the published ones never drift.
#
#   https://hecaton.org.uk/schema/config/hecaton-config-5.7.xsd        (and .json)
#   https://hecaton.org.uk/schema/client-config/hecaton-client-config-5.7.xsd
#   https://hecaton.org.uk/schema/client-config/hecaton-client-failover-config-5.7.xsd
#   https://hecaton.org.uk/schema/spring/hecaton-spring-5.7.xsd        (and hecaton-spring.xsd)
set -euo pipefail

SITE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SITE")"
DIST="$SITE/dist"
CORE="$ROOT/hecaton/src/main/resources"
SPRING="$ROOT/hecaton-spring/src/main/resources"

rm -rf "$DIST"
mkdir -p "$DIST/schema/config" "$DIST/schema/client-config" "$DIST/schema/spring"
cp -R "$SITE/static/." "$DIST/"

cp "$CORE"/hecaton-config-*.xsd "$CORE"/hecaton-config-*.json "$DIST/schema/config/"
cp "$CORE"/hecaton-client-config-*.xsd "$CORE"/hecaton-client-failover-config-*.xsd "$DIST/schema/client-config/"
cp "$SPRING"/hecaton-spring-*.xsd "$DIST/schema/spring/"

# The unversioned Spring schema is the latest one, as in META-INF/spring.schemas.
latest_spring="$(sed -n 's|^https\\://hecaton.org.uk/schema/spring/hecaton-spring.xsd=||p' \
    "$SPRING/META-INF/spring.schemas")"
cp "$SPRING/$latest_spring" "$DIST/schema/spring/hecaton-spring.xsd"

# A plain listing page per schema directory, and one for /schema/.
list_page() {
    local dir="$1" title="$2"
    {
        cat <<HTML
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>$title · Hecaton</title>
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Press+Start+2P&family=VT323&display=swap">
<link rel="stylesheet" href="/style.css">
</head>
<body class="listing">
<main class="wrap">
<p><a href="/">&larr; hecaton.org.uk</a></p>
<h1>$title</h1>
<ul class="files">
HTML
        shift 2
        for entry in "$@"; do
            echo "<li><a href=\"$entry\">$entry</a></li>"
        done
        cat <<HTML
</ul>
</main>
</body>
</html>
HTML
    } > "$dir/index.html"
}

for ns in config client-config spring; do
    files=($(cd "$DIST/schema/$ns" && ls | sort -V))
    list_page "$DIST/schema/$ns" "/schema/$ns/" "${files[@]}"
done
list_page "$DIST/schema" "/schema/" config/ client-config/ spring/

echo "Built $DIST: $(find "$DIST" -type f | wc -l) files"
