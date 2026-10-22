# Releasing Hecaton

A release is built by the `Release` workflow (`.github/workflows/release.yml`) when a tag
`v<version>` is pushed to a commit on `main` whose CI is green:

```sh
git tag -s v1.0.0 -m "Hecaton 1.0.0"
git push origin v1.0.0
```

The workflow runs [`tools/release/release.sh`](../tools/release/release.sh), which

1. sets the project version (`versions:set`) in the checkout; the POMs on `main` keep the
   `-SNAPSHOT` version;
2. builds every module with the `release` profile: sources and Javadoc jars, per-jar
   attribution files, GPG signatures;
3. with the Central secrets present, deploys to Maven Central (profile `enable-maven-central`);
   the deployment is validated and then published by a maintainer in the
   [Central portal](https://central.sonatype.com/) (add `-Pauto-release-maven-central` to publish
   it automatically);
4. writes a CycloneDX SBOM of the release (`hecaton-<version>-sbom.json` and `.xml`);
5. writes the release notes and collects the distributions, the `hecaton-migrate` CLI, the SBOM
   and their SHA-256 checksums in `target/release`.

It then tests the member image, pushes it (amd64 and arm64) to `ghcr.io/l0ngsight/hecaton` as
`<version>` and, for a final version, `latest`, and creates the GitHub release with the files of
`target/release`. A version with a suffix (`1.1.0-rc.1`) is published as a pre-release. If the
release was already created for the tag (for example from the GitHub UI, which also pushes the
tag), its title, notes and pre-release flag are kept and the files are uploaded to it.

Running the workflow by hand (`workflow_dispatch`) with a version builds the release without
publishing anything: the files are kept as a workflow artifact. Locally:

```sh
tools/release/release.sh 1.0.0 --no-sign      # dry run; see target/release
```

## Repository secrets

| Secret | Used for |
|---|---|
| `CENTRAL_USERNAME`, `CENTRAL_TOKEN` | A Central portal user token for the `uk.org.hecaton` namespace (needs `hecaton.org.uk` verified, D7) |
| `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE` | The release signing key (ASCII-armoured), published on a key server |

Without them the workflow still builds and publishes the image and the GitHub release, unsigned
and without Maven Central.

## Release notes

[`tools/release/release-notes.py`](../tools/release/release-notes.py) lists the pull requests
merged since the previous tag, then the commits made directly on `main`, grouped by type for
subjects in the conventional form (`feat(wan): ...`, `fix: ...`) and by area for subjects
written as `Area: summary` (the form CONTRIBUTING asks for). Edit the GitHub release afterwards
to add highlights and upgrade notes.

## Versions

The product version (`1.0.0`) is independent of the codebase version members and clients
negotiate (`hecaton.codebase.version`, 5.7.0; spec §3.2). After a release, `main` moves to the
next `-SNAPSHOT` version in an ordinary pull request.
