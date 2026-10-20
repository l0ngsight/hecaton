# Merge Changes Summary

The Hecaton and Hecaton Jet products were merged together to provide a
seamless experience. Changes done during the merge of Hecaton and Hecaton
Jet codebases are summarized in this document.

Hecaton Jet repository was merged into Hecaton repository.

The name Jet doesn't refer to standalone product anymore, but is kept when
referring to the streaming engine part of the product - the Jet engine.

Hecaton repository - https://github.com/hazelcast/hazelcast
Hecaton Jet repository - https://github.com/hazelcast/hazelcast-jet

After some preparatory commits removing conflicting files (e.g. root pom.xml),
the merge was performed by the following command:

```bash
$ git merge --allow-unrelated-histories hecaton-jet/master
``` 

This preserves history for both projects and allows easy rebasing of patches
both ways (forward porting and backporting from/to Hecaton Jet).

Hecaton repository continues its development as with a next major version.

## Module changes

- `hecaton-jet-core` code was merged into `hecaton` module
- `hecaton-jet-spring` module was merged into `hecaton-spring` and removed
- `hecaton-jet-distribution` module was removed, `hecaton-distribution`
  module was adapted to produce very similar artifacts - full and slim
  distribution
- the extension modules were kept under `extensions/*`, keeping the groupId and
  artifactId coordinates, this makes it easy for existing users to migrate.
- `examples` modules were deleted, they will be merged with imdg example, which
  are stored in a separate repository at https://github.com/hazelcast/hazelcast-code-samples/
- `hecaton-jet-sql` module was merged into `hecaton-sql` module (this
  actually happened after the merge, but for users it is indistinguishable).`

## Code Changes

- JetNodeExtension was merged with DefaultNodeExtension
- The Jet service is started when a HecatonInstance is started
- HecatonInstance can return JetInstance via #getJetInstance (this will
  change before the release into a `JetService getJet()` where `JetService`
  provides a subset of `JetInstance` methods)
- Jet data structures created by uk.org.hecaton.jet.impl.JobRepository are
  created lazily when needed
- JetConfig is now a field in `uk.org.hecaton.config.Config`
- Jet run tests in parallel by default, Jet tests were marked with `QuickTest`
  and `ParallelJVMTest` `@Category` accordingly
- IMDG's `smallInstanceConfig()` for tests sets the
  `uk.org.hecaton.jet.config.InstanceConfig#setCooperativeThreadCount` to 2
- Jet cooperative threads are suspended if no jobs are running.
- One can disable Jet code upload for security reasons.

## Checkstyle

- Jet code was adapted to IMDG checkstyle rules
- some checkstyle rules were relaxed

There are 2 outstanding items to resolve:
- some checkstyle rules were ignored for Jet code
- Jet had some stricter rules regarding public javadoc, this is now not in
  place, ideally we should apply it to the whole product

## Distribution

- IMDG and Jet distributions were merged, the result is closer to the Jet
  distribution as there were more features.
- the distribution is started using `bin/hecaton-start` command
- the cli tool is under `bin/hecaton` (used to be `bin/jet`)
- the configuration files
  - hecaton.yaml - configuration file for hecaton, contains also
    configuration for the Jet engine (follows the structure where the JetConfig
    is a field of Config)
  - hecaton-client.yaml - configuration file for the CLI tool


